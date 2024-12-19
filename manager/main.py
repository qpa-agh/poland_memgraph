import time
from importing.importing_data import DATA_LOADERS

time.sleep(1)
IMPORT_COMMAND = "import"
EXIT_COMMAND = "exit"


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


def run_cli():
    print('=============================================================================')
    print("Welcome to CLI Tool!\nEnter a command. Type 'exit' to quit.")
    print(
        f"Usage with automatic file detection: \n\t{IMPORT_COMMAND} auto <data_type1> [data_type2 ...] or '{IMPORT_COMMAND} auto all'"
    )
    print(
        f"Usage with providing file names: \n\t{IMPORT_COMMAND} <data_type1> <file_name1>  [data_type2 file_name2...]"
    )
    print(f"<data_type> = [{', '.join(DATA_LOADERS.keys())}]")
    print(f"All files should be in csv format and be located in \data directory")
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
            else:
                print(
                    f"Unknown command. Available commands: {IMPORT_COMMAND} <data_type1> [data_type2 ...], exit."
                )
        except KeyboardInterrupt:
            print("\nExiting the CLI tool.")
            break


if __name__ == "__main__":
    run_cli()
