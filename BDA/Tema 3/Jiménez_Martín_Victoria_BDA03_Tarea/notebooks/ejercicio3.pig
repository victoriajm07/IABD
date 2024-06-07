
REGISTER piggybank.jar

-- Leemos el fichero fligths.csv

FLIGHTS = LOAD '$flights_file' USING
       org.apache.pig.piggybank.storage.CSVExcelStorage(',', 'NO_MULTILINE', 'UNIX', 'SKIP_INPUT_HEADER')
       AS (dayofmonth:int, dayofweek:int, carrier:chararray, 
               depairportid:chararray, arrairportid:chararray, depdelay:int, arrdelay:int);

-- Filtramos los vuelos que salieron con retraso
DELAYED_FLIGHTS = FILTER FLIGHTS BY depdelay > 15;

-- Filtramos los vuelos que salieron con retraso pero que llegaron con 15 minutos o menos de retraso
RECOVERED_FLIGHTS = FILTER FLIGHTS BY depdelay > 15 AND arrdelay <= 15;

-- Agrupamos los vuelos retrasados por aerolínea
GROUPED_DELAYED = GROUP DELAYED_FLIGHTS BY carrier;
GROUPED_RECOVERED = GROUP RECOVERED_FLIGHTS BY carrier;
-- Contamos los vuelos retrasados y los vuelos recuperados por aerolínea
COUNT_DELAYED = FOREACH GROUPED_DELAYED GENERATE group AS carrier, COUNT(DELAYED_FLIGHTS) AS total_delayed;
COUNT_RECOVERED = FOREACH GROUPED_RECOVERED GENERATE group AS carrier, COUNT(RECOVERED_FLIGHTS) AS total_recovered;

-- Realizamos un JOIN por aerolínea para tener ambos conteos en la misma tupla
JOINED = JOIN COUNT_DELAYED BY carrier, COUNT_RECOVERED BY carrier;

-- Calculamos el porcentaje de recuperación
CALCULATED_PERCENTAGE = FOREACH JOINED GENERATE
    COUNT_DELAYED::carrier AS carrier,
    ((float)COUNT_RECOVERED::total_recovered / (float)COUNT_DELAYED::total_delayed) AS percent_recovered;

-- Ordenamos las aerolíneas por el porcentaje de recuperación de mayor a menor
ORDERED_RECOVERY = ORDER CALCULATED_PERCENTAGE BY percent_recovered DESC;

-- Limitamos a las 5 aerolíneas principales
TOP_5_RECOVERY = LIMIT ORDERED_RECOVERY 5;

-- Mostramos el resultado
DUMP TOP_5_RECOVERY;
