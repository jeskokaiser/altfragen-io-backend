"""
Quota and credit error detection for AI APIs.

Detects when an API has run out of credits/quota and should trigger
automatic feature disabling.
"""

import re
from typing import Optional


def is_quota_error(error: Exception, api_name: str = "") -> bool:
    """
    Detect if an error indicates quota/credit exhaustion.
    
    Args:
        error: The exception that occurred
        api_name: Optional API name for context-specific detection
    
    Returns:
        True if this appears to be a quota/credit error
    """
    error_str = str(error).lower()
    error_type = type(error).__name__
    
    # Common patterns across APIs
    quota_patterns = [
        r"quota",
        r"credit",
        r"insufficient.*fund",
        r"payment.*required",
        r"billing",
        r"account.*limit",
        r"rate.*limit.*exceeded",
        r"429",  # HTTP 429 Too Many Requests
        r"resource.*exhausted",
        r"exceeded.*quota",
        r"out.*of.*credit",
        r"balance.*insufficient",
    ]
    
    # Check for quota-related patterns
    for pattern in quota_patterns:
        if re.search(pattern, error_str, re.IGNORECASE):
            return True
    
    # API-specific checks
    if api_name.lower() == "openai":
        openai_patterns = [
            r"insufficient_quota",
            r"billing_not_active",
            r"rate_limit_exceeded",
            r"invalid_api_key",
        ]
        for pattern in openai_patterns:
            if re.search(pattern, error_str, re.IGNORECASE):
                return True
    
    elif api_name.lower() == "gemini":
        gemini_patterns = [
            r"resource_exhausted",
            r"quotafailure",
            r"generaterequestsperday",
            r"exceeded.*current.*quota",
        ]
        for pattern in gemini_patterns:
            if re.search(pattern, error_str, re.IGNORECASE):
                return True
    
    elif api_name.lower() == "mistral":
        mistral_patterns = [
            r"quota.*exceeded",
            r"rate.*limit",
            r"429",
        ]
        for pattern in mistral_patterns:
            if re.search(pattern, error_str, re.IGNORECASE):
                return True
    
    elif api_name.lower() in ["perplexity", "deepseek"]:
        # Both use similar error formats
        instant_patterns = [
            r"429",
            r"rate.*limit",
            r"quota.*exceeded",
            r"insufficient.*credit",
        ]
        for pattern in instant_patterns:
            if re.search(pattern, error_str, re.IGNORECASE):
                return True
    
    # Check HTTP status codes in error messages
    if "429" in error_str or "402" in error_str:  # Too Many Requests, Payment Required
        return True
    
    # Check for specific exception types that indicate quota issues
    if "RateLimitError" in error_type or "QuotaExceeded" in error_type:
        return True
    
    return False


def extract_quota_message(error: Exception, api_name: str = "") -> str:
    """
    Extract a user-friendly message about the quota error.
    
    Args:
        error: The exception that occurred
        api_name: Optional API name
    
    Returns:
        A formatted message about the quota issue
    """
    error_str = str(error)
    
    # Try to extract meaningful parts
    if "429" in error_str:
        return f"{api_name or 'API'} rate limit exceeded (HTTP 429)"
    elif "402" in error_str:
        return f"{api_name or 'API'} payment required (HTTP 402)"
    elif "quota" in error_str.lower():
        return f"{api_name or 'API'} quota exhausted"
    elif "credit" in error_str.lower():
        return f"{api_name or 'API'} credits exhausted"
    elif "billing" in error_str.lower():
        return f"{api_name or 'API'} billing issue"
    else:
        return f"{api_name or 'API'} quota/credit error: {error_str[:100]}"

