import time
from importing.importing_data import DATA_LOADERS
from database.communication import execute_query
from queries.query_runners import QUERY_RUNNERS
from settings import toggle_clear_preprocessed
from relationships.relationship_creation import RELATIONSHIP_CREATORS
import signal
import sys
import os
import shutil
import readline

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
RUN_CUSTOM_QUERY_COMMAND = 'cq'
RUN_QUERY_COMMAND = 'q'
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
        print("Creating all relationships...\n")
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
                    f"Unknown relationship id: '{argument}'. Available options: {', '.join(RELATIONSHIP_CREATORS.keys())}, all."
                )
                break

    print("\nRelationship creation Report:")
    for name, duration in report:
        print(f"Relationship {name.capitalize()} created in {duration:.2f} seconds.")
    print(
        f"Total time taken: {sum([duration for _, duration in report]):.2f} seconds.\n"
    )
    
def run_query(arguments):
    report = []
    if "preset" in arguments:
        return
        print("Running preset queries...\n")
        
        print("\Query running report:")
        for name, duration in report:
            print(f"Query {name.capitalize()} run in {duration:.2f} seconds.")
        print(
            f"Total time taken: {sum([duration for _, duration in report]):.2f} seconds.\n"
        )
        return
    query_no = arguments[0]
    args = arguments[1:]
    if query_no in QUERY_RUNNERS:
        duration = measure_time(QUERY_RUNNERS[query_no], *args)
        report.append((query_no, duration))
    else:
        print(f"Unknown query type: '{query_no}'. Available options: {', '.join(QUERY_RUNNERS.keys())}, all.")
    print(f"Query {query_no.capitalize()} run in {duration:.2f} seconds.")

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
    print(f"Usage: {RUN_QUERY_COMMAND} <query_no1> [argument_no1 argument_no2 ...] or '{RUN_QUERY_COMMAND} preset'")
    print(f"")
    print(f"Use command '{CLEAR_DATABASE_COMMAND}' to clear all data in the database")
    print(f"Use command '{RUN_CUSTOM_QUERY_COMMAND} <query>' to run a custom query")
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
            if command[:len(EXIT_COMMAND)].lower() == EXIT_COMMAND:
                print("Exiting the CLI tool.")
                break

            if command[:len(IMPORT_COMMAND)].lower() == IMPORT_COMMAND:
                parts = command.split()
                if len(parts) > 1:
                    arguments = parts[1:]
                    import_data(arguments)
                else:
                    print(
                        f"Usage: {IMPORT_COMMAND} <data_type1> [data_type2 ...] or '{IMPORT_COMMAND} all'"
                    )
            elif command[:len(CREATE_RELATIONSHIP_COMMAND)].lower() == CREATE_RELATIONSHIP_COMMAND:
                parts = command.split()
                if len(parts) > 1:
                    arguments = parts[1:]
                    create_relationships(arguments)
                else:
                    print(
                        f"Usage: {CREATE_RELATIONSHIP_COMMAND} <relationship_no1> [relationship_no2 ...] or '{CREATE_RELATIONSHIP_COMMAND} all'"
                    )
            elif command[:len(RUN_QUERY_COMMAND)].lower() == RUN_QUERY_COMMAND:
                parts = command.split()
                if len(parts) > 1:
                    arguments = parts[1:]
                    run_query(arguments)
                else:
                    print(
                        f"Usage: {RUN_QUERY_COMMAND} <query_no1> [argument_no1 argument_no2 ...] or '{RUN_QUERY_COMMAND} all'"
                    )
            elif command[:len(RUN_CUSTOM_QUERY_COMMAND)].lower() == RUN_CUSTOM_QUERY_COMMAND:
                query = command[len(RUN_CUSTOM_QUERY_COMMAND):]
                execute_query(query, return_full=True)
            elif command[:len(CLEAR_DATABASE_COMMAND)].lower() == CLEAR_DATABASE_COMMAND:
                execute_query('MATCH (n) DETACH DELETE n')
            elif command[:len(TOGGLE_PREPROCESSED_DATA_CLEANING)].lower() == TOGGLE_PREPROCESSED_DATA_CLEANING:
                toggle_clear_preprocessed()
            elif command[:len(REMOVE_COMMAND)].lower() == REMOVE_COMMAND:
                parts = command.split()
                if parts[1] == 'dir':
                    dir = os.path.join('/data', parts[2])
                    shutil.rmtree(dir)
                    if os.path.exists(dir):
                        os.rmdir(dir)
                elif parts[1] == 'file':
                    file = os.path.join('/data', parts[2])
                    os.remove(file)
            elif command[:len(HELP_COMMAND)].lower() == HELP_COMMAND:
                print_help()
            else:
                print(f"Unknown command.")
                print(f"Enter '{HELP_COMMAND}' to see commands")
                
        except KeyboardInterrupt:
            print("\nExiting the CLI tool.")
            break


if __name__ == "__main__":
    run_cli()
