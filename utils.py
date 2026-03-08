import argparse
import asyncio
from cli.parallel_upload import command as parallel_upload
from cli.secuential_upload import command as secuential_upload
from cli.clean import command as clean

def main():   
    parser = argparse.ArgumentParser(description="CLI for file batch uploading")
    subparsers = parser.add_subparsers(dest="command", help="Availables commands")

    parser_clean = subparsers.add_parser('clean', help="Local resources cleaning")
    parser_clean.add_argument('--container', required=True, type=str, help="Container")

    parser_upload = subparsers.add_parser('parallel-upload', help="Upload files to the cloud")
    parser_upload.add_argument('--container', required=True, type=str, help="Container")
    parser_upload.add_argument('--samples', required=True, type=int, default=5, help="Samples quantity")
    parser_upload.add_argument('--run-name', required=True, type=str, help="Run name")

    parser_upload = subparsers.add_parser('secuential-upload', help="Upload files to the cloud")
    parser_upload.add_argument('--container', required=True, type=str, help="Container")
    parser_upload.add_argument('--samples', required=True, type=int, default=5, help="Samples quantity")
    parser_upload.add_argument('--run-name', required=True, type=str, help="Run name")

    args = parser.parse_args()

    if args.command == 'clean':
        asyncio.run(clean(container=args.container))
    
    elif args.command == 'parallel-upload':
        asyncio.run(parallel_upload(
            container=args.container, 
            samples=args.samples, 
            run_name=args.run_name
        ))
    elif args.command == 'secuential-upload':
        asyncio.run(secuential_upload(
            container=args.container, 
            samples=args.samples, 
            run_name=args.run_name
        ))
    else:
        parser.print_help() 

if __name__ == "__main__":
    main()
