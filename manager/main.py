import time
from importing.importing_data import DATA_LOADERS
from database.communication import execute_query
from settings import toggle_clear_preprocessed
from relationships.relationship_creation import RELATIONSHIP_CREATORS
import signal
import sys
import os
import shutil

def signal_handler(sig, frame):
    print('You pressed Ctrl+C!')
    sys.exit(0)

signal.signal(signal.SIGINT, signal_handler)
signal.signal(signal.SIGTERM, signal_handler)

time.sleep(0.5)
IMPORT_COMMAND = "import"
CREATE_RELATIONSHIP_COMMAND = "cr"
EXIT_COMMAND = "exit"
CLEAR_DATABASE_COMMAND = 'delete_all'
RUN_QUERY_COMMAND = 'query'
TOGGLE_PREPROCESSED_DATA_CLEANING = 'clear_preprocessed'
REMOVE_COMMAND = "srm"
HELP_COMMAND = 'help'

def measure_time(func, *args, **kwargs):
    start_time = time.time()
    func(*args, **kwargs)
    end_time = time.time()
    duration = end_time - start_time
    return duration


def import_data(arguments):
    report = []

    if arguments[0] == "auto":
        arguments = arguments[1:]
        if "all" in arguments:
            print("Importing all data...\n")
            for name, loader in DATA_LOADERS.items():
                duration = measure_time(loader)
                report.append((name, duration))
        else:
            for argument in arguments:
                if argument in DATA_LOADERS:
                    duration = measure_time(DATA_LOADERS[argument])
                    report.append((argument, duration))
                else:
                    print(
                        f"Unknown data type: '{argument}'. Available options: {', '.join(DATA_LOADERS.keys())}, all."
                    )
                    break

    else:
        current_option = None
        for argument in arguments:
            if argument in DATA_LOADERS:
                current_option = argument
            elif current_option is not None and argument.endswith("csv"):
                file_name = argument
                duration = measure_time(DATA_LOADERS[current_option], name=file_name[:-4])
                report.append((argument, duration))
            else:
                print(
                    f"Unknown data type: '{argument}'. Available options: {', '.join(DATA_LOADERS.keys())}, all."
                )
                break

    print("\nImport Report:")
    for name, duration in report:
        print(f"{name.capitalize()} data imported in {duration:.2f} seconds.")
    print(
        f"Total time taken: {sum([duration for _, duration in report]):.2f} seconds.\n"
    )

def create_relationships(arguments):
    report = []
    if "all" in arguments:
        print("Importing all data...\n")
        for name, loader in RELATIONSHIP_CREATORS.items():
            duration = measure_time(loader)
            report.append((name, duration))
    else:
        for argument in arguments:
            if argument in RELATIONSHIP_CREATORS:
                duration = measure_time(RELATIONSHIP_CREATORS[argument])
                report.append((argument, duration))
            else:
                print(
                    f"Unknown data type: '{argument}'. Available options: {', '.join(RELATIONSHIP_CREATORS.keys())}, all."
                )
                break

    print("\nRelationship creation Report:")
    for name, duration in report:
        print(f"relationship {name.capitalize()} created in {duration:.2f} seconds.")
    print(
        f"Total time taken: {sum([duration for _, duration in report]):.2f} seconds.\n"
    )
    
def print_help():
    print(f"All files should be in csv format and be located in \data directory")
    print(
        f"Usage with automatic file detection: \n\t{IMPORT_COMMAND} auto <data_type1> [data_type2 ...] or '{IMPORT_COMMAND} auto all'"
    )
    print(
        f"Usage with providing file names: \n\t{IMPORT_COMMAND} <data_type1> <file_name1>  [data_type2 file_name2...]"
    )
    print(f"<data_type> = [{', '.join(DATA_LOADERS.keys())}]")
    print(f"")
    print(f"To create relationships use '{CREATE_RELATIONSHIP_COMMAND}'")
    print(f"Usage: {CREATE_RELATIONSHIP_COMMAND} <relationship_no1> [relationship_no2 ...] or '{CREATE_RELATIONSHIP_COMMAND} all'")
    print(f"")
    print(f"Use command '{CLEAR_DATABASE_COMMAND}' to clear all data in the database")
    print(f"Use command '{RUN_QUERY_COMMAND} <query>' to run a custom query")
    print(f"""Use command '{TOGGLE_PREPROCESSED_DATA_CLEANING}' to toggle between clearing modes. 
          If clearing mode is off then already processed data will not be recalculated.
          Default is off.""")

def run_cli():
    print('=============================================================================')
    print("Welcome to CLI Tool!\nEnter a command. Type 'exit' to quit.")
    print(f"All files should be in csv format and be located in \data directory")
    print(f"Enter '{HELP_COMMAND}' to see commands")
   
    
    while True:
        try:
            command = input("> ").strip()
            if command.lower() == EXIT_COMMAND:
                print("Exiting the CLI tool.")
                break

            if command.startswith(IMPORT_COMMAND):
                parts = command.split()
                if len(parts) > 1:
                    arguments = parts[1:]
                    import_data(arguments)
                else:
                    print(
                        f"Usage: {IMPORT_COMMAND} <data_type1> [data_type2 ...] or '{IMPORT_COMMAND} all'"
                    )
            elif command.startswith(CREATE_RELATIONSHIP_COMMAND):
                parts = command.split()
                if len(parts) > 1:
                    arguments = parts[1:]
                    create_relationships(arguments)
                else:
                    print(
                        f"Usage: {CREATE_RELATIONSHIP_COMMAND} <relationship_no1> [relationship_no2 ...] or '{CREATE_RELATIONSHIP_COMMAND} all'"
                    )
            elif command.startswith(RUN_QUERY_COMMAND):
                query = command[len(RUN_QUERY_COMMAND):]
                execute_query(query)
            elif command.startswith(CLEAR_DATABASE_COMMAND):
                execute_query('MATCH (n) DETACH DELETE n')
            elif command.startswith(TOGGLE_PREPROCESSED_DATA_CLEANING):
                toggle_clear_preprocessed()
            elif command.startswith(REMOVE_COMMAND):
                parts = command.split()
                if parts[1] == 'dir':
                    dir = os.path.join('/data', parts[2])
                    shutil.rmtree(dir)
                    if os.path.exists(dir):
                        os.rmdir(dir)
                elif parts[1] == 'file':
                    file = os.path.join('/data', parts[2])
                    os.remove(file)
            elif command.startswith(HELP_COMMAND):
                print_help()
            else:
                print(f"Unknown command.")
                print(f"Enter '{HELP_COMMAND}' to see commands")
                
        except KeyboardInterrupt:
            print("\nExiting the CLI tool.")
            break


if __name__ == "__main__":
    run_cli()
