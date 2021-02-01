import sass
import os
import argparse

def check_extension(file_name, extension):
    if not file_name:
        return False
    
    extensions = file_name.split('.')
    
    if len(extensions) < 2:
        return False
    
    if extensions[-1] == extension:
        return True
    
    return False

def check_file(file_name):
    return os.path.isfile(file_name)

def error_message(message = None):
    print("Usage: python src\compile_sass.py app\static\input.scss app\static\output.css [options]")
    if message:
        print(message)
    exit(-1)
    
def check_args():
    parser = argparse.ArgumentParser("python src\compile_sass.py")
    parser.add_argument('-i', required=True)
    parser.add_argument('-o', required=True)
    parser.add_argument('--new', action='store_true', default = True)
    parser.add_argument('--overwrite', action='store_true')

    args = parser.parse_args()
    infile = args.i
    outfile = args.o

    if not check_extension(infile, 'scss'):
        error_message()
    
    if not check_extension(outfile, 'css'):
        error_message()
    
    if not check_file(infile):
        error_message()
    
    if not args.overwrite and check_file(outfile):
        error_message("If you want to overwrite please specify the --overwrite argument")
    
    return infile, outfile

def main():
    infile, outfile = check_args()
    with open(infile, 'r') as f:
        scss = f.read()
    css = sass.compile(string=scss)

    with open(outfile, 'w') as f:
        f.write(css)

if __name__ == '__main__':
    main()