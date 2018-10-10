import json
import csv
import ast
import datetime
import sys
import optparse

def main(opts):
    now = datetime.datetime.now().strftime('%s')+".csv"
    f = csv.writer(open(now, "wb+"))
    f.writerow(["received_timestamp","UNIX_received","sent_timestamp", "UNIX_sent","diff", "vin", "power_ID", "event", "rpm_value", "odometer_value", "total_fuel_value", "fuel_level_value", "speed_value", "engine_hours_value", "ignition", "jbus_connected", "esn", "wheels_in_motion", "city", "lat", "long", "description", "hdop", "country", "time_since_update", "state", "GPS_valid", "satellites", "accuracy", "heading","event_type"])

    if len(sys.argv) > 1:
        with open(opts.input_file) as my_file:
            for line in my_file.readlines():
               try:
                    if 'Sending:' in line:
                        if "count\'" in line:
                            columns = line.split(' [DEBUG] Sending:')
                            k1 = json.dumps(ast.literal_eval(columns[1].strip()))
                            entry = json.loads(k1)
                            f.writerow([columns[0],
                                        datetime.datetime.strptime(columns[0], '%Y-%m-%d %H:%M:%S.%f').strftime('%s.%f'),
                                        datetime.datetime.fromtimestamp((entry['timestamp'] / 1000.00) + (opts.time_zone * 3600)).strftime('%Y-%m-%d %H:%M:%S.%f'),
                                        entry['timestamp']/1000.00,
                                        (float((datetime.datetime.strptime(columns[0], '%Y-%m-%d %H:%M:%S.%f').strftime('%s.%f')))-(entry['timestamp']/1000.0)),
                                        entry['data'][0]['powerunit_vin'],
                                        entry['data'][0]['powerunit_id'],
                                        entry['event'],
                                        entry['data'][0]['engine_parameters']['rpm']['value'],
                                        entry['data'][0]['engine_parameters']['odometer']['value'],
                                        entry['data'][0]['engine_parameters']['total_fuel_used']['value'],
                                        entry['data'][0]['engine_parameters']['fuel_level']['value'],
                                        entry['data'][0]['engine_parameters']['speed']['value'],
                                        entry['data'][0]['engine_parameters']['engine_hours']['value'],
                                        entry['data'][0]['ignition'],
                                        entry['data'][0]['jbus_connected'],
                                        entry['data'][0]['esn'],
                                        entry['data'][0]['wheels_in_motion'],
                                        entry['data'][0]['location']['city'],
                                        entry['data'][0]['location']['lat'],
                                        entry['data'][0]['location']['lon'],
                                        entry['data'][0]['location']['description'],
                                        entry['data'][0]['location']['hdop'],
                                        entry['data'][0]['location']['country'],
                                        entry['data'][0]['location']['time_since_update'],
                                        entry['data'][0]['location']['state'],
                                        entry['data'][0]['location']['valid'],
                                        entry['data'][0]['location']['satellites'],
                                        entry['data'][0]['location']['accuracy'],
                                        entry['data'][0]['location']['heading']
                                        ])
                        else:
                            columns = line.split(' [DEBUG] Sending:')
                            k1 = json.dumps(ast.literal_eval(columns[1].strip()))
                            entry = json.loads(k1)
                            f.writerow([columns[0],
                                        datetime.datetime.strptime(columns[0], '%Y-%m-%d %H:%M:%S.%f').strftime('%s.%f'),
                                        datetime.datetime.fromtimestamp((entry['timestamp'] / 1000.00) + (opts.time_zone * 3600)).strftime('%Y-%m-%d %H:%M:%S.%f'),
                                        entry['timestamp']/1000.00,
                                        (float((datetime.datetime.strptime(columns[0], '%Y-%m-%d %H:%M:%S.%f').strftime('%s.%f'))) - (entry['timestamp'] / 1000.0)),
                                        None,
                                        None,
                                        'periodic_update',
                                        entry['engine_parameters']['rpm']['value'],
                                        entry['engine_parameters']['odometer']['value'],
                                        None,
                                        None,
                                        entry['engine_parameters']['speed']['value'],
                                        entry['engine_parameters']['engine_hours']['value'],
                                        None,
                                        None,
                                        None,
                                        None,
                                        entry['location']['city'],
                                        entry['location']['lat'],
                                        entry['location']['lon'],
                                        entry['location']['description'],
                                        entry['location']['hdop'],
                                        entry['location']['country'],
                                        entry['location']['time_since_update'],
                                        entry['location']['state'],
                                        entry['location']['valid'],
                                        entry['location']['satellites'],
                                        entry['location']['accuracy'],
                                        entry['location']['heading'],
                                        None])
               except:
                  f.writerow([columns[0],
                              datetime.datetime.strptime(columns[0], '%Y-%m-%d %H:%M:%S.%f').strftime('%s.%f'),
                              datetime.datetime.fromtimestamp((entry['timestamp'] / 1000.00) + (opts.time_zone * 3600)).strftime('%Y-%m-%d %H:%M:%S.%f'),
                              entry['timestamp']/1000.00,
                              (float((datetime.datetime.strptime(columns[0], '%Y-%m-%d %H:%M:%S.%f').strftime('%s.%f'))) - (entry['timestamp'] / 1000.0)),
                              None,
                              None,
                              entry['event'],
                              None,
                              None,
                              None,
                              None,
                              None,
                              None,
                              None,
                              None,
                              None,
                              None,
                              None,
                              None,
                              None,
                              None,
                              None,
                              None,
                              None,
                              None,
                              None,
                              None,
                              None,
                              None,
                              entry['type']])
    else:
        exit('No file provided')

if __name__ == '__main__':
    # This If Passes only if the currently file is directly run by the user and not imported by another file.
    DESCRIPTION = """
        converting json logs to .csv format
         """

    parser = optparse.OptionParser(description=DESCRIPTION)
    # creating a object for the option parser

    parser.add_option('-f', '--input_file', dest='input_file', help='Input Log File')
    parser.add_option('-t', '--time_zone', dest='time_zone', type=int, help='Time Change from Time Zone of Input File')

    (opts, args) = parser.parse_args()

    main(opts)
