import pytest
from palindrome_check import is_palindrome


def test_palindrome_success():
    string = "raCecar"

    expected_res = True

    assert is_palindrome(string) == expected_res


def test_palindrome_fail():
    string = "raCsecar"

    expected_res = False

    assert is_palindrome(string) == expected_res
