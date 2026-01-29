from decimal import Decimal, InvalidOperation
import math


def safe_decimal(value, default='0'):
    """
    Convert value to Decimal, handling None, NaN, and invalid values.
    
    Args:
        value: Value to convert (can be None, str, int, float, Decimal, etc.)
        default: Default value to return if conversion fails (default: '0')
    
    Returns:
        Decimal: Converted value or default Decimal if conversion fails
    """
    if value is None:
        return Decimal(default)
    try:
        val_str = str(value).strip()
        if val_str.lower() in ('nan', 'none', '', 'inf', '-inf'):
            return Decimal(default)
        val = Decimal(val_str)
        # Kiểm tra NaN bằng cách convert sang float
        try:
            float_val = float(val)
            if math.isnan(float_val) or math.isinf(float_val):
                return Decimal(default)
        except (ValueError, OverflowError):
            return Decimal(default)
        return val
    except (InvalidOperation, ValueError, TypeError, OverflowError):
        return Decimal(default)


def safe_float(value, default=0.0):
    """
    Convert value to float, handling None, NaN, and invalid values.
    
    Args:
        value: Value to convert (can be None, str, int, float, Decimal, etc.)
        default: Default value to return if conversion fails (default: 0.0)
    
    Returns:
        float: Converted value or default float if conversion fails
    """
    if value is None:
        return default
    try:
        val = float(value)
        if math.isnan(val) or math.isinf(val):
            return default
        return val
    except (ValueError, TypeError, OverflowError):
        return default
