"""代码渠道说明 / 最小样例单一真相源测试。"""

from providers.code_docs import get_code_channel_docs


def test_code_docs_load_markdown_and_real_samples():
    result = get_code_channel_docs()
    assert result["doc"].startswith("# 代码渠道")
    assert "先判断：你属于哪一种情况" in result["doc"]

    samples = {item["id"]: item for item in result["samples"]}
    assert set(samples) == {"minimal"}
    for item in samples.values():
        assert item["title"]
        assert item["summary"]
        assert item["hooks"]
        assert "class " in item["code"]
