
from mrjob.job import MRJob
import csv

class VisitorPointsMRJob(MRJob):

    def mapper(self, _, line):
        row = dict(zip(['Div', 'Date', 'Time','HomeTeam', 'AwayTeam', 'FTHG', 'FTAG', 'FTR'], next(csv.reader([line]))))

        visitor_team = row['AwayTeam']
        points = 0
        if row['FTR'] == 'A':
            points = 3
        elif row['FTR'] == 'D':
            points = 1

        yield visitor_team, points

    def reducer(self, team, points):
        yield team, sum(points)

if __name__ == '__main__':
    VisitorPointsMRJob.run()
