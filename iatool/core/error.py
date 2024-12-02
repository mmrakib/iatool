def format_error_message(type: str, message: str) -> str:
    return f"{type}: {message}"

class APIError(Exception):
    def __init__(self, message: str):
        self.message = message
        self.error_message = format_error_message("APIError", message)

        super().__init__(self.error_message)

    def __str__(self) -> str:
        return self.error_message

class InputError(Exception):
    def __init__(self, message: str):
        self.message = message
        self.error_message = format_error_message("InputError", message)

        super().__init__(self.error_message)

    def __str__(self) -> str:
        return self.error_message
