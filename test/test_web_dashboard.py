import sys
import yaml
from types import ModuleType


def test_dashboard_uses_current_user(monkeypatch, tmp_path):
    app_module = ModuleType("app")

    class DummyApp:
        def route(self, *_args, **_kwargs):
            def decorator(func):
                return func
            return decorator

        def after_request(self, func):
            return func

        def logger(self, *_args, **_kwargs):
            return None

    class DummyLoginManager:
        def user_loader(self, func):
            return func

    app_module.app = DummyApp()
    app_module.login_manager = DummyLoginManager()
    sys.modules["app"] = app_module

    user_module = ModuleType("user")
    user_module.User = object
    sys.modules["user"] = user_module

    misc_module = ModuleType("misc")
    misc_module.update_cache = lambda *_args, **_kwargs: None
    misc_module.send_test_mail_ = lambda *_args, **_kwargs: None
    misc_module.send_author_publications = lambda *_args, **_kwargs: None
    misc_module.get_file_for_dois = lambda *_args, **_kwargs: None
    misc_module.get_user_dois = lambda *_args, **_kwargs: set()
    sys.modules["misc"] = misc_module

    from web import routes

    user_id = "3523285"
    user_dir = tmp_path / f"user_{user_id}"
    user_dir.mkdir(parents=True, exist_ok=True)
    user_yaml = user_dir / "metadata.yaml"
    user_yaml.write_text(yaml.safe_dump(["Ada Lovelace"]), encoding="utf-8")

    monkeypatch.setattr(routes, "get_user_dir", lambda _user_id: tmp_path / f"user_{_user_id}")
    tracked_authors, ignored_dois, department_org_ids, cache_last_modified, talks_last_modified, latest_collection = routes._load_user_dashboard_data(user_id)

    assert tracked_authors == "Ada Lovelace"
    assert ignored_dois == ""
    assert department_org_ids == ""
    assert cache_last_modified
    assert talks_last_modified
    assert latest_collection is None
