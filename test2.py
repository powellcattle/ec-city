import openpyxl

wb = openpyxl.load_workbook(r"data/Incode/190325 Water Accts.xlsx", read_only=True)

ws = wb["Water Accounts"]

for row in ws:
    if row[0].value == "MIU":
        continue
    miu = row[0].value
    account = row[4].value
    if account is None:
        continue
    meter = row[5].value

    try:
        if row[6].value is None:
            continue
        current = int(row[6].value)
    except ValueError:
        continue

    try:
        if row[7].value is None:
            continue
        previous = int(row[7].value)
    except ValueError:
        continue

    consumption = current - previous
    print(f"{account} {consumption}")
