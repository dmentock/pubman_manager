from pubman_manager.doi_parser import DOIParser


def test_affiliation_reuses_similar_publisher_affiliation_without_pure_history():
    dp = DOIParser.__new__(DOIParser)
    dp.authors_affiliation_counters = {}

    affiliations_by_author = {
        ("Alice", "Smith"): ["Department of Materials Science, University X"],
        ("Bob", "Jones"): ["Materials Science Department, University X"],
    }

    results = DOIParser.compare_author_list_to_pure_db(
        dp,
        affiliations_by_author,
        fuzz_threshold=80,
    )

    alice_aff = results[("Alice", "Smith")][0].affiliation
    bob_aff = results[("Bob", "Jones")][0].affiliation

    assert alice_aff == bob_aff
