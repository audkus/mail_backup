import json
from collections import defaultdict

INPUT_FILE = "email_backup.json"


def main():
    year_counts = defaultdict(int)
    total = 0

    print("🔍 Analyzing emails by year...")
    with open(INPUT_FILE, 'r', encoding='utf-8') as f:
        data = json.load(f)
        for email in data:
            date = email.get('date')
            if date and len(date) >= 4:
                year = date[:4]
                year_counts[year] += 1
            else:
                year_counts["unknown"] += 1
            total += 1

    print(f"\n📊 Total emails analyzed: {total}")
    for year in sorted(year_counts):
        print(f"  {year}: {year_counts[year]}")


if __name__ == "__main__":
    main()
