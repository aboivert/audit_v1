VISUALIZATION_REGISTRY = {}

def visualization_function(category, name, description="", parameters=None):
    def decorator(func):
        if category not in VISUALIZATION_REGISTRY:
            VISUALIZATION_REGISTRY[category] = []
        
        VISUALIZATION_REGISTRY[category].append({
            'function': func,
            'name': name,
            'description': description,
            'parameters': parameters or {},
            'function_name': func.__name__
        })
        return func
    return decorator