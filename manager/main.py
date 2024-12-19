import sys
import time
from importing.importing_data import DATA_LOADERS

time.sleep(1)
IMPORT_COMMAND = "import"
EXIT_COMMAND = "exit"

def measure_time(func):
    start_time = time.time()
    func()
    end_time = time.time()
    duration = end_time - start_time
    return duration

def import_data(arguments):
    report = []  

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
                print(f"Unknown data type: '{argument}'. Available options: {', '.join(DATA_LOADERS.keys())}, all.")

    print("\nImport Report:")
    for name, duration in report:
        print(f"{name.capitalize()} data imported in {duration:.2f} seconds.")
    print(f"Total time taken: {sum([duration for _, duration in report]):.2f} seconds.\n")

def main():
    print("CLI Tool: Enter a command. Type 'exit' to quit.")
    print(f"Usage: {IMPORT_COMMAND} <data_type1> [data_type2 ...] or '{IMPORT_COMMAND} all'")
    print(f"<data_type> = [all, {', '.join(DATA_LOADERS.keys())}]")
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
                    print(f"Usage: {IMPORT_COMMAND} <data_type1> [data_type2 ...] or '{IMPORT_COMMAND} all'")
            else:
                print(f"Unknown command. Available commands: {IMPORT_COMMAND} <data_type1> [data_type2 ...], exit.")
        except KeyboardInterrupt:
            print("\nExiting the CLI tool.")
            break


if __name__ == "__main__":
    main()