import csv
from faker import Faker
import datetime


def data_generate(records, headers):
    """
    function to generate dummy data for a specified number of records
    :param records:
    :param headers:
    :return:
    """
    fake = Faker('en_US')
    fake1 = Faker('en_GB')  # To generate phone numbers
    with open("Sample1.csv", 'wt') as csvFile:
        writer = csv.DictWriter(csvFile, fieldnames=headers)
        writer.writeheader()
        for i in range(records):
            full_name = fake.name()
            f_lname = full_name.split(" ")
            fname = f_lname[0]
            lname = f_lname[1]
            domain_name = "@testDomain.com"
            user_id = fname + "." + lname + domain_name

            writer.writerow({
                "Email Id": user_id,
                "Prefix": fake.prefix(),
                "Name": fake.name(),
                "Birth Date": fake.date(pattern="%d-%m-%Y", end_datetime=datetime.date(2000, 1, 1)),
                "Phone Number": fake1.phone_number(),
                "Additional Email Id": fake.email(),
                "Address": fake.address(),
                "Zip Code": fake.zipcode(),
                "City": fake.city(),
                "State": fake.state(),
                "Country": fake.country(),
                "Year": fake.year(),
                "Time": fake.time(),
                "Link": fake.url(),
                "Text": fake.word(),
            })


if __name__ == '__main__':
    records = 10000000
    headers = ["Email Id", "Prefix", "Name", "Birth Date", "Phone Number", "Additional Email Id",
               "Address", "Zip Code", "City", "State", "Country", "Year", "Time", "Link", "Text"]
    data_generate(records, headers)
    print("CSV generation complete!")
