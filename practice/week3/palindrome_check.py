import logging
logger = logging.getLogger(__name__)

def is_palindrome(string):

    try:
        string = string.lower()
        left = 0
        right = len(string) -1
        
        while left < right:
            if string[left] != string[right]:
                logger.info(f"The word {string} is not a palindrome")
                return False
            left += 1
            right -= 1
        

        logger.info(f"The word {string} is a palindrome")
        return True
    except:
        logger.error("Something unexpected broke the function")



string1 = "Racee eCar"

print(is_palindrome(string1))



list1 = ["hi", "whats up?", "Nothing much", "how about you?"]
#print(sum(list1))


total = ""
for number in list1:
    total += number

print(total)


def main():
    logging.basicConfig(filename="palindrome.log", level=logging.INFO)
    is_palindrome("String")
    is_palindrome("racecar")
    is_palindrome(234)
    list2 = ["race", "racecar", "hello"]
    print(list(map(is_palindrome, list2)))


if __name__ == '__main__':
    main()


