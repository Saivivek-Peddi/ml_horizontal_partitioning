import json

# User Validation
with open('users.json') as f:
    users = json.load(f)

usrs=[]
for item in users:
    usrs.append(item['username'])

user = input("Enter Username: ")
while user not in usrs:
    print("Invalid username. Please enter proper username")
    user = input("Enter Username: ")

for item in users:
    if user==item['username']:
        user = item
        break


# Query
print('')
print(user)




