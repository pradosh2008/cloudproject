def string_reverse(str1):
    #Returns the reversed String.

    #Parameters:
    #    str1 (str):The string which is to be reversed.

    #Returns:
    #    reverse(str1):The string which gets reversed.

    reverse_str1 = ''
    i = len(str1)
    while i > 0:
        reverse_str1 += str1[i - 1]
        i = i- 1
    return reverse_str1

#print(string_reverse.__doc__)

#help(string_reverse)


def square(a):
    '''Returned argument a is squared.'''
    return a**a

# print (square.__doc__)
#
# help(square)


def some_function(argument1):
    """Summary or Description of the Function

    Parameters:
    argument1 (int): Description of arg1

    Returns:
    int:Returning value

   """

    return argument1

# print(some_function.__doc__)


#follow this

def string_reverse(str1):
    '''
    Returns the reversed String.

    Parameters:
        str1 (str):The string which is to be reversed.

    Returns:
        reverse(str1):The string which gets reversed.
    '''

    reverse_str1 = ''
    i = len(str1)
    while i > 0:
        reverse_str1 += str1[i - 1]
        i = i- 1
    return reverse_str1

# print(string_reverse('DeepLearningDataCamp'))
#
# help(string_reverse)


# import math
# print(math.__doc__)


class Vehicles(object):
    '''
    The Vehicle object contains a lot of vehicles

    Args:
        arg (str): The arg is used for...
        *args: The variable arguments are used for...
        **kwargs: The keyword arguments are used for...

    Attributes:
        arg (str): This is where we store arg,
    '''
    def __init__(self, arg, *args, **kwargs):
        self.arg = arg

    def cars(self, distance,destination):
        '''We can't travel distance in vehicles without fuels, so here is the fuels

        Args:
            distance (int): The amount of distance traveled
            destination (bool): Should the fuels refilled to cover the distance?

        Raises:
            RuntimeError: Out of fuel

        Returns:
            cars: A car mileage
        '''
        pass

print(Vehicles.cars.__doc__)