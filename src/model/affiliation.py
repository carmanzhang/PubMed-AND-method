from enum import Enum


class Affiliation:
    """Class that represents an Affiliation"""

    class TYPE(Enum):
        """Possible Affiliation Types"""
        UNIVERS = 1
        INSTITU = 2
        COLLEGE = 3
        LABOR = 4
        ORGANI = 5
        MINISTRY = 6
        CENTER = 7
        DEPARTMENT = 8
        HOSPITAL = 9
        SCHOOL = 10

    class DESCRIPTOR(Enum):
        """Possible Affiliation Descriptors"""
        BIOLOG = 1
        CHEMIST = 2
        PEDIATRIC = 3
        SURGERY = 4
        MEDIC = 5
        GENETIC = 6
        INFECT = 7
        AGRICULT = 8
        ENTOMOLOG = 9
        BIOTECH = 10
        NEUROLOG = 11
        PSYCHOL = 12
        PHARMA = 13
        TOXIC = 14
        CANCER = 15
        CARDIOL = 16
        DENTIST = 17
        NUTRITION = 18
        HEALTH = 19
        DISEASE = 20

    # Type & Descriptor finder methods
    @staticmethod
    def find_type(text: str):
        """Returns the Affiliation Type by searching in the given string"""
        if text is None:
            return None

        temp = text.upper()

        for t in Affiliation.TYPE:
            if t.name in temp:
                return t
        return None

    @staticmethod
    def find_descriptor(text):
        """Returns the Affiliation Descriptor by searching in the given string"""
        if text is None:
            return None

        temp = text.upper()

        for d in Affiliation.DESCRIPTOR:
            if d.name in temp:
                return d
        return None
