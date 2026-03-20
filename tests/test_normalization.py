from src.normalization import join_snippet_texts, normalize_text


def test_normalize_text_collapses_whitespace():
    assert normalize_text("hello\n\tworld   test") == "hello world test"


def test_join_snippet_texts_combines_snippets():
    snippets = [{"text": "hello  world"}, {"text": " more\ntext"}]
    assert join_snippet_texts(snippets) == "hello world more text"
