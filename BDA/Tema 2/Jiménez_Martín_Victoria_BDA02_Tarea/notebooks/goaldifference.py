
from mrjob.job import MRJob
from mrjob.step import MRStep
import csv

class GoalDifferenceMRJob(MRJob):

    def steps(self):
        return [
            MRStep(mapper=self.mapper,
                   reducer=self.reducer),
            MRStep(reducer=self.reducer_find_goal_difference)
        ]

    def mapper(self, _, line):
        if line.startswith('Div'):
            return
        
        # Ignorar líneas mal formateadas o la cabecera del CSV
        try:
            row = list(csv.reader([line]))[0]
            if len(row) < 7:
                return

            home_team = row[3]
            away_team = row[4]
            home_goals = int(row[5])
            away_goals = int(row[6])

            yield home_team, home_goals
            yield away_team, away_goals

        except ValueError:
            pass  # Saltar líneas que no pueden ser procesadas

    def reducer(self, team, goals):
        total_goals = sum(goals)
        yield None, (team, total_goals)

    def reducer_find_goal_difference(self, _, team_goals):
        min_goals = float('inf')
        max_goals = 0
        team_min_goals = None
        team_max_goals = None

        for team, goals in team_goals:
            if goals < min_goals:
                min_goals = goals
                team_min_goals = team
            if goals > max_goals:
                max_goals = goals
                team_max_goals = team
        
        # Verificar si se encontraron equipos válidos
        yield "Equipo con mas goles", (team_max_goals, max_goals)
        yield "Equipo con menos goles", (team_min_goals, min_goals)
        yield "Diferencia de goles", max_goals - min_goals

if __name__ == '__main__':
    GoalDifferenceMRJob.run()
