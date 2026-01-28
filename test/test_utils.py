import pytest
import datetime as dt
from typing import Any, List

import utils


@pytest.mark.parametrize("elements, number, expected_chunks", [
    ([0, 1, 2, 3, 4, 5, 6, 7, 8, 9], 3, [[0, 3, 6, 9], [1, 4, 7], [2, 5, 8]]),
    ([0, 1, 2, 3, 4, 5, 6, 7, 8, 9], 4, [[0, 4, 8], [1, 5, 9], [2, 6], [3, 7]]),
    (["one", "two", "three", "four", "five", "six", "seven"], 4, [["one", "five"], ["two", "six"], ["three", "seven"], ["four"]]),
    ([False, False, True, True], 3, [[False, True], [False], [True]])
])
def test_chunk_list(elements: List[Any], number: int, expected_chunks: List[List[Any]]) -> None:
    # Arrange
    # Act
    chunks = utils.chunk_list(elements, number)

    # Assert
    assert len(chunks) == len(expected_chunks)
    for chunk, expected_chunk in zip(chunks, expected_chunks):
        assert len(chunk) == len(expected_chunk)
        for element, expected_element in zip(chunk, expected_chunk):
            assert element == expected_element


@pytest.mark.parametrize("file_name, expected_datetime", [
    ("reddits_corgi_2020-01-01T00:00:00_2021-01-01T00:00:00", dt.datetime(2021, 1, 1, 0, 0, 0)),
    ("reddits_corgi_2021-01-01T00:00:00_2022-01-01T00:00:00", dt.datetime(2022, 1, 1, 0, 0, 0)),
    ("reddits_corgi_2024-01-01T00:00:00_2025-01-01T00:00:00", dt.datetime(2025, 1, 1, 0, 0, 0)),
    ("reddits_corgi_2026-01-01T00:00:00_2027-01-01T00:00:00", dt.datetime(2027, 1, 1, 0, 0, 0)),
])
def test_get_file_date_from_file_name(file_name: str, expected_datetime: dt.datetime) -> None:
    # Arrange
    # Act
    result_datetime = utils.get_file_date_from_file_name(file_name)

    # Assert
    assert result_datetime == expected_datetime
