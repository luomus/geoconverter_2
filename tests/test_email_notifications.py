import sys
import os
from unittest.mock import patch, MagicMock

# Add the src directory to the path so we can import our modules
sys.path.insert(0, 'src/')

# Provide a lightweight fake settings module to avoid pydantic dependency in tests
from unittest.mock import MagicMock
mock_settings = MagicMock()
_dummy = MagicMock()
_dummy.EMAIL_ENABLED = False
_dummy.ADMIN_EMAILS = ''
_dummy.FROM_EMAIL = 'noreply@example.com'
_dummy.SMTP_HOST = 'localhost'
_dummy.SMTP_PORT = 25
_dummy.SMTP_TLS = False
_dummy.SMTP_USER = ''
_dummy.SMTP_PASSWORD = ''
_dummy.CONVERTER_ENV = 'test'
_dummy.LOGGING = 'INFO'
mock_settings.Settings.return_value = _dummy
import sys
sys.modules['settings'] = mock_settings

from email_notifications import send_simple_email, notify_failure


def test_send_email():
    """Test that email function works without actually sending."""
    with patch('email_notifications.app_settings') as mock_settings:
        mock_settings.EMAIL_ENABLED = False
        
        result = send_simple_email("Test", "Test message")
        
        assert result is False


def test_notify_failure():
    """Test that notify_failure function works."""
    with patch('email_notifications.send_simple_email') as mock_send:
        mock_send.return_value = True
        
        notify_failure("Test error", "TEST123")
        
        mock_send.assert_called_once()


if __name__ == "__main__":
    test_send_email()
    test_notify_failure()
    print("All tests passed!")
