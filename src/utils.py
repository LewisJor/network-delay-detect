import os

def ensure_directory_exists(file_path: str) -> None:
    """
    Ensures the directory for the given file path exists.
    Creates all necessary parent directories if they don't exist.
    
    Args:
        file_path (str): The file path for which to ensure the directory exists
    """
    directory = os.path.dirname(file_path)
    if directory and not os.path.exists(directory):
        try:
            os.makedirs(directory, exist_ok=True)
            print(f"[INFO] Created directory: {directory}")
        except Exception as e:
            print(f"[ERROR] Failed to create directory {directory}: {e}") 