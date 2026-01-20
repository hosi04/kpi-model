class Constants:
    # Base year for revenue calculation
    BASE_YEAR: int = 2025
    
    # KPI year target
    KPI_YEAR_2026: int = 2026
    
    # Total KPI target for 2026
    KPI_TOTAL_2026: float = 50000000000

    DATE_LABELS = [
                'Normal day',  # Baseline
                'Double Day',
                'Double Day +1',
                'Double Day -1',
                'Middle of month',
                'Middle of month +1',
                'Middle of month -1',
                'Pay Day',
                'Pay Day +1',
                'Pay Day -1'
            ]   

# Create a default instance for convenience
constants = Constants()

