from pathlib import Path

import cli as cli_module


def test_generate_talks_template_uses_user_config_and_output(monkeypatch, tmp_path):
    user_id = "3523285"
    user_dir = tmp_path / f"user_{user_id}"
    user_dir.mkdir(parents=True, exist_ok=True)
    metadata_path = user_dir / "metadata.yaml"
    metadata_path.write_text("department_org_ids:\n  - ou_1863336\n", encoding="utf-8")

    monkeypatch.setattr(cli_module, "USER_DATA_DIR", tmp_path)

    captured = {}

    def fake_generate_talks_template(resolved_user_id, org_ids, output_path=None):
        captured["user_id"] = resolved_user_id
        captured["org_ids"] = list(org_ids)
        captured["output_path"] = output_path
        return output_path

    monkeypatch.setattr(cli_module, "generate_talks_template", fake_generate_talks_template)

    output_path = tmp_path / "talks" / "new_template.xlsx"
    result = cli_module.main(
        [
            "generate-talks-template",
            "--user-id",
            user_id,
            "--output",
            str(output_path),
        ]
    )

    assert result == 0
    assert captured == {
        "user_id": user_id,
        "org_ids": ["ou_1863336"],
        "output_path": output_path,
    }
