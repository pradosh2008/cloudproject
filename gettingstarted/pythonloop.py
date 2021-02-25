# colors = {"red", "green", "blue", "purple"}
# i = 0
# print(len(colors))
# while i < len(colors):
#     print(colors[i])
#     i += 1

# from itertools import count
# multiples_of_five = count(step=5)
# print(type(multiples_of_five))
# print(multiples_of_five)
#
# for n in multiples_of_five:
#     if n > 100:
#         break
#     print(n)

# for item in {"red", "green", "blue", "purple"}:
#     print(item)

# fruits = {
#     'apple': 1,
#     'banana': 2,
#     'coconut': 3
# }
#
# fruits['apple']


# building deck
# def build_deck():
#     numbers = list(range(2, 15))
#     # print(numbers)
#     suits=['H', 'S', 'C', 'D']
#     deck = []
#     for i in numbers:
#         for s in suits:
#             card = s+str(i)
#             deck.append(card)
#     return deck
#
#
# deck = build_deck()
# print(deck)
# print(len(deck))

#build_deck()

# def is_palindrome(num):
#     '''
#     Checks the number is a palindrome or not . Returns True or False .
#
#     Parameters:
#         num (int):The number which is to be checked .
#
#     Returns:
#         True or False
#
#     lets say number is 151
#     divide number%10
#     i=0
#     if number%10 == 0
#          then False
#     else
#        while temp>0:
#            i=i*10+number%10
#            temp=temp//10
#     print(i)
#     1
#     15
#     1*10+5
#     5
#     15*10+
#
#     '''
#     i = 0
#     temp = num
#     if num // 10 == 0:
#         print("0 is not a palindrome")
#         return False
#     else:
#         while temp > 0:
#             i = i*10+temp % 10
#             temp = temp//10
#     if i == num:
#         print("{} is a palindrome".format(num))
#         return True
#     else:
#         print("{} is not a palindrome".format(num))
#         return False
#
# is_palindrome(-121)


# def is_palindrome_v2(num):
#     # Skip single-digit inputs
#     if num // 10 == 0:
#         return False
#     temp = num
#     reversed_num = 0
#
#     while temp != 0:
#         reversed_num = (reversed_num * 10) + (temp % 10)
#         print(reversed_num)
#         temp = temp // 10
#         print(temp)
#
#     if num == reversed_num:
#         print(num)
#         return num
#
#     else:
#         print(num)
#         return False
#
# is_palindrome_v2(-121)  #this has a flaw, for negative number .

headers=['id','name','add']
columns=[[1,2],['ram','d'],['bbsr','post']]

# for header, rows in zip(headers, columns):
#     print("{}: {}".format(header,rows))
#
#
# for header, rows in zip(headers, columns):
#     print("{}: {}".format(header, ", ".join(rows)))


s=91,2,3
# for i in s:
#     print(i)

#print(len(range(s)))
# print("-----------------------")
# for i in range(len(s)):
#     print(s[i])


# s={91,2,3}
# for i in range(len(s)):
#     print(s[i])  #TypeError: 'set' object is not subscriptable

# def hello():
#     print("hi")
#
# a=hello()
# print(type(a))
#
# new_val = 10
# def square(value):
#     """Returns the square of a number."""
#     #new_val=2
#     new_value2 = new_val ** 2
#     return new_value2
# a=square(3)
# print(a)


# #global new_val
# new_val=10
# def square(value):
#     """Returns the square of a number."""
#     global new_val
#     new_val = new_val ** 2
#     return new_val
# print(square(3))


# def func1():
#     num = 3
#     print(num)
#
# def func2():
#     global num   #NameError: name 'num' is not defined
#     double_num = num * 2
#     num = 6
#     print(double_num)
#
# func1()
# func2()

# def raise_val(n):
#     """Return the inner function."""
#     def inner(x):
#         """Raise x to the power of n."""
#         raised = x ** n
#         return raised
#     return inner
#
# square = raise_val(2)
# cube = raise_val(3)
# print(square(2), cube(4))


# def outer(num):
#     x=num
#     print(x)
#     def inner():
#         #print("inner : "+str(x))
#         =x+1
#     inner()
#     print("outer : "+str(x))
#
# outer(4)

echo_word="hi"
# Define echo_shout()
def echo_shout(word):
    """Change the value of a nonlocal variable"""

    # Concatenate word with itself: echo_word
    echo_word = word + word

    # Print echo_word
    print(echo_word)

    # Define inner function shout()
    def shout():
        """Alter a variable in the enclosing scope"""
        # Use echo_word in nonlocal scope
        global echo_word
        # echo_word=word
        # Change echo_word to echo_word concatenated with '!!!'
        echo_word = echo_word + "!!!"
        print(echo_word)

    # Call function shout()
    shout()

    # Print echo_word
    print(echo_word)


# Call function echo_shout() with argument 'hello'
echo_shout('hello')