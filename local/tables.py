from xerparser import Xer

def main():
    file = r"updated.xer"
    with open(file, encoding=Xer.CODEC, errors="ignore") as f:
        file_contents = f.read()
    xer = Xer(file_contents)
    print("Table Names:")
    for names in xer.table_names:
        print(names)
        # print("\n")

if __name__ == "__main__":
    main()