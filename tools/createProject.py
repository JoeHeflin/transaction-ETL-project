import os

# Define the project structure as a dictionary
project_structure = {
    "config": ["source_schema_mapping.json", "transformation_rules.yaml"],
    "data": ["raw/", "processed/", "final/"],
    "logs": [],
    "src": {
        "ingestion": [
            "ingest_from_source_a.py",
            "ingest_from_source_b.py",
            "common_ingestion.py",
        ],
        "transformation": [
            "transform_source_a.py",
            "transform_source_b.py",
            "common_transformation.py",
        ],
        "manual_processing": ["manual_processor.py", "validation.py"],
        "utils": ["schema_validator.py", "logging_util.py", "file_util.py"],
        "main.py": [],
    },
    "tests": [],
    "requirements.txt": [],
    "README.md": [],
}


# Function to create project structure
def create_structure(base_dir, structure):
    for key, value in structure.items():
        # Create directories
        dir_path = os.path.join(base_dir, key)
        os.makedirs(dir_path, exist_ok=True)
        if isinstance(value, dict):
            # If it's a dictionary, it represents a directory with subdirectories/files
            create_structure(dir_path, value)  # Recursive call for subdirectories
        else:
            # Create directory if key is just a folder name (e.g., "logs", "data/raw")
            # if key.endswith("/"):
            #     os.makedirs(dir_path, exist_ok=True)
            # Otherwise, create files
            # else:
            #     if not os.path.exists(dir_path):
            #         with open(dir_path, 'w') as f:
            #             f.write("")  # Create an empty placeholder file
            # Create all files listed in the list for this directory
            for file_name in value:
                file_path = os.path.join(dir_path, file_name)
                if file_name.endswith("/"):
                    os.makedirs(file_path, exist_ok=True)
                if not os.path.exists(file_path):
                    with open(file_path, "w") as f:
                        f.write("")  # Create an empty file


# Function to execute project creation
def create_project():
    print("Creating project structure...")
    base_dir = "data_processing_project"
    os.makedirs(base_dir, exist_ok=True)
    create_structure(base_dir, project_structure)
    print(f"Project structure created at {os.path.abspath(base_dir)}")


if __name__ == "__main__":
    create_project()
