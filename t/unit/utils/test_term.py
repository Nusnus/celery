import os
from base64 import b64encode
from tempfile import NamedTemporaryFile
from unittest.mock import patch

import pytest

import t.skip
from celery.utils import term
from celery.utils.term import _read_as_base64, colored, fg, supports_images


@t.skip.if_win32
class test_colored:

    @pytest.fixture(autouse=True)
    def preserve_encoding(self, patching):
        patching('sys.getdefaultencoding', 'utf-8')

    @pytest.mark.parametrize('name,color', [
        ('black', term.BLACK),
        ('red', term.RED),
        ('green', term.GREEN),
        ('yellow', term.YELLOW),
        ('blue', term.BLUE),
        ('magenta', term.MAGENTA),
        ('cyan', term.CYAN),
        ('white', term.WHITE),
    ])
    def test_colors(self, name, color):
        assert fg(30 + color) in str(colored().names[name]('foo'))

    @pytest.mark.parametrize('name', [
        'bold', 'underline', 'blink', 'reverse', 'bright',
        'ired', 'igreen', 'iyellow', 'iblue', 'imagenta',
        'icyan', 'iwhite', 'reset',
    ])
    def test_modifiers(self, name):
        assert str(getattr(colored(), name)('f'))

    def test_unicode(self):
        assert str(colored().green('∂bar'))
        assert colored().red('éefoo') + colored().green('∂bar')
        assert colored().red('foo').no_color() == 'foo'

    def test_repr(self):
        assert repr(colored().blue('åfoo'))
        assert "''" in repr(colored())

    def test_more_unicode(self):
        c = colored()
        s = c.red('foo', c.blue('bar'), c.green('baz'))
        assert s.no_color()
        c._fold_no_color(s, 'øfoo')
        c._fold_no_color('fooå', s)

        c = colored().red('åfoo')
        assert c._add(c, 'baræ') == '\x1b[1;31m\xe5foo\x1b[0mbar\xe6'

        c2 = colored().blue('ƒƒz')
        c3 = c._add(c, c2)
        assert c3 == '\x1b[1;31m\xe5foo\x1b[0m\x1b[1;34m\u0192\u0192z\x1b[0m'

    def test_read_as_base64(self):
        test_data = b"The quick brown fox jumps over the lazy dog"
        with NamedTemporaryFile(mode='wb') as temp_file:
            temp_file.write(test_data)
            temp_file.seek(0)
            temp_file_path = temp_file.name

            result = _read_as_base64(temp_file_path)
            expected_result = b64encode(test_data).decode('ascii')

            assert result == expected_result

    @pytest.mark.parametrize('is_tty, iterm_profile, expected', [
        (True, 'test_profile', True),
        (False, 'test_profile', False),
        (True, None, False),
    ])
    @patch('sys.stdin.isatty')
    @patch.dict(os.environ, {'ITERM_PROFILE': 'test_profile'}, clear=True)
    def test_supports_images(self, mock_isatty, is_tty, iterm_profile, expected):
        mock_isatty.return_value = is_tty
        if iterm_profile is None:
            del os.environ['ITERM_PROFILE']
        assert supports_images() == expected
