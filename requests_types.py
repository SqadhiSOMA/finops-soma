ressource_group_last_week = {
    "type": "ActualCost",
    "timeframe": "LastYear",
    "dataset": {
        "granularity": "Daily",
        "aggregation": {
            "totalCost": {
                "name": "PreTaxCost",
                "function": "Sum"
            }
        },
        "grouping": [
            {
                "type": "Dimension",
                "name": "ResourceGroup"
            }
        ]
    }
}

service_name_last_week = {
    "type": "ActualCost",
    "timeframe": "WeekToDate",
    "dataset": {
        "granularity": "Daily",
        "aggregation": {
            "totalCost": {
                "name": "PreTaxCost",
                "function": "Sum"
            }
        },
        "grouping": [
            {
                "type": "Dimension",
                "name": "ServiceName"
            }
        ]
    }
}


ressource_last_week = {
    "type": "Usage",
    "timeframe": "WeekToDate",
    "dataset": {
        "granularity": "Daily",
        "aggregation": {
            "totalCost": {
                "name": "PreTaxCost",
                "function": "Sum"
            }
        },
        "grouping": [
            {
                "type": "Dimension",
                "name": "ResourceId"
            }
        ]
    }
}
