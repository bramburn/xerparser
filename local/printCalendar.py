from xerparser import Xer


def main():
    file = r"updated.xer"
    with open(file, encoding=Xer.CODEC, errors="ignore") as f:
        file_contents = f.read()
    xer = Xer(file_contents)

    for clndr_id, clndr_name, clndr_data in zip(xer.calendar_df['clndr_id'],
                                                xer.calendar_df['clndr_name'],
                                                xer.calendar_df['clndr_data'],
                                                ):
        print(f"{clndr_id} - {clndr_name} =  {clndr_data}")


if __name__ == "__main__":
    main()
