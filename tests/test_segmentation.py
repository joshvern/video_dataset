from src.segmentation.wired_5_levels import WiredFiveLevelsStrategy


def test_wired_strategy_splits_known_markers_into_levels():
    snippets = [
        {"text": "Level 1: I would explain physics to a child using toys."},
        {"text": "More child details here."},
        {"text": "Level 2: For a teenager, physics gets more mathematical."},
        {"text": "Level 3: A college student can use calculus."},
        {"text": "Level 4: A grad student should reason from first principles."},
        {"text": "Level 5: An expert will talk quantum field theory."},
    ]

    segments = WiredFiveLevelsStrategy().segment(snippets, metadata={})

    assert [segment.level_key for segment in segments] == [
        "child",
        "teen",
        "college_student",
        "grad_student",
        "expert",
    ]
    assert segments[0].content.startswith("Level 1")
    assert "teenager" in segments[1].content
    assert segments[-1].confidence > 0.5
