
REGISTER piggybank.jar

FLIGHTS = LOAD '$flights_file' USING org.apache.pig.piggybank.storage.CSVExcelStorage(',', 'NO_MULTILINE', 'UNIX', 'SKIP_INPUT_HEADER')
    AS (dayofmonth:int, dayofweek:int, carrier:chararray, flightnum:int, depdelay:int, arrdelay:int);

-- Filtrar los vuelos que tienen un retraso mayor a 15 minutos
delayed_flights = FILTER FLIGHTS BY arrdelay > 15;

-- Agrupar los vuelos retrasados por aerolínea
grouped_flights = GROUP delayed_flights BY carrier;

-- Contar los vuelos retrasados por aerolínea
counted_flights = FOREACH grouped_flights GENERATE group AS carrier, COUNT(delayed_flights) AS delayed_flights;

-- Ordenar las aerolíneas por la cantidad de vuelos retrasados de mayor a menor
sorted_flights = ORDER counted_flights BY delayed_flights DESC;

-- Limitar los resultados a las cinco principales aerolíneas
top_five_carriers = LIMIT sorted_flights 5;

-- Almacenar o mostrar los resultados
DUMP top_five_carriers;
