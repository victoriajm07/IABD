
from mrjob.job import MRJob

class MaxGrades(MRJob):

    def mapper(self, _, line):
        student, *grades = line.split()
        grades = [int(grade) for grade in grades]
        yield student, max(grades)

    def reducer(self, student, grades):
        yield student, max(grades)

if __name__ == '__main__':
    MaxGrades.run()
