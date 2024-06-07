
#!/usr/bin/python3
#!/usr/bin/env python3
from mrjob.job import MRJob
from collections import deque

class TeamPointsMRJob(MRJob):

    def mapper(self, _, line):
        # Dividir la línea y extraer los campos necesarios
        row = line.split(',')
        if row[0] == 'Div':  # Saltar la cabecera
            return
        try:
            home_team = row[3]
            away_team = row[4]
            home_goals = int(row[5])
            away_goals = int(row[6])
            result = 'H' if home_goals > away_goals else 'D' if home_goals == away_goals else 'A'

            # Emitir puntos y resultado para el equipo local y visitante
            if result == 'H':
                yield home_team, ('W', 3)
                yield away_team, ('L', 0)
            elif result == 'D':
                yield home_team, ('D', 1)
                yield away_team, ('D', 1)
            elif result == 'A':
                yield away_team, ('W', 3)
                yield home_team, ('L', 0)

        except ValueError:
            pass  # Saltar líneas mal formadas

    def reducer(self, team, values):
        total_points = 0
        last_five = deque(maxlen=5)
        for result, points in values:
            total_points += points
            # Convertir 'W', 'D', 'L' a valores numéricos y añadir a last_five
            numeric_result = 3 if result == 'W' else 1 if result == 'D' else 0
            last_five.append(numeric_result)
        yield team, (total_points, list(last_five))

if __name__ == '__main__':
    TeamPointsMRJob.run()
