
-- registramos la librería PiggyBank para poder usar la función de carga CSVExcelStorage.
REGISTER piggybank.jar

-- Leemos el fichero de vuelos.csv
FLIGHTS = LOAD '$flights_file' USING org.apache.pig.piggybank.storage.CSVExcelStorage(',', 'NO_MULTILINE', 'UNIX', 'SKIP_INPUT_HEADER')
    AS (dayofmonth:int, dayofweek:int, carrier:chararray, flightnum:int, depdelay:int, arrdelay:int);
    
vuelos_retrasados = FILTER FLIGHTS BY arrdelay > 15;
vuelos_por_aerolinea = GROUP vuelos_retrasados BY carrier;
conteo_retrasos = FOREACH vuelos_por_aerolinea GENERATE group AS carrier, COUNT(vuelos_retrasados) AS total_retrasos;
vuelos_ordenados = ORDER conteo_retrasos BY total_retrasos DESC;
top_5_retrasos = LIMIT vuelos_ordenados 5;
DUMP top_5_retrasos;
