import pandas as pd
import json
import logging
from new_validation import (
    BreadCrumbValidator,
    EventStopValidator,
    IntegratedValidator
)

class Collector:

    def __init__(self):
        self.breadcrumb_storage = []
        self.stop_event_storage = []

        self.breadcrumb_shipped = False
        self.stop_event_shipped = False

    def receive(self, item):
        """ Consumer received a dict, and this methods identify the dict
            type and classify to the correct storage.

        :param item: received dict.
        """
        if len(item) == 24:
            self.receive_stop_event(item)
        elif len(item) == 14:
            self.receive_breadcrumb(item)
        else:
            logging.error(f'Data Format Error.\n{item}')

    def receive_breadcrumb(self, breadcrumb_dict):
        """ Consumer received a breadcrumb dict object which has been 
            parsed from bytes into python dict. Append it to storage.
        """
        self.breadcrumb_storage.append(breadcrumb_dict)

    def receive_stop_event(self, stop_event_dict):
        """ Consumer received a stop event dict object which has been 
            parsed from bytes into python dict. Append it to storage.
            Only interested field (load into db) will be saved.
        """
        self.stop_event_storage.append({
            'trip_id': stop_event_dict['trip_id'],
            'vehicle_number': stop_event_dict['vehicle_number'],
            'route_number': stop_event_dict['route_number'],
            'direction': stop_event_dict['direction'],
            'service_key': stop_event_dict['service_key']
        })

    def breadcrumb_has_shipped(self):
        """ Reserved temp fn which maybe useful as a semophore 
            in multithreading or -processing
        """
        self.breadcrumb_shipped = True

    def stop_event_has_shipped(self):
        """ Reserved temp fn which maybe useful as a semophore 
            in multithreading or -processing
        """
        self.stop_event_shipped = True

    def integrate(self):
        """ Call it when consumer has no more message to consume.
            Output a list of dicts that have been verified and 
            transformed, which are ready for batched loading into db.
            Clear the in-memory dicts. 
        """
        df_breadcrumb = pd.DataFrame.from_dict(self.breadcrumb_storage)
        df_event_stop = pd.DataFrame.from_dict(self.stop_event_storage)

        df_breadcrumb = BreadCrumbValidator(df_breadcrumb).validate()
        df_event_stop = EventStopValidator(df_event_stop).validate()

        integrated_validator = IntegratedValidator(df_breadcrumb, df_event_stop)
        df_integration = integrated_validator.validate()
        ready_to_load = df_integration.to_dict('records')

        self.breadcrumb_storage = []
        self.stop_event_storage = []

        return ready_to_load


if __name__ == '__main__':
    with open('2021-02-23.json') as f:
        stops = json.load(f)
        f.close()

    with open('2021-02-23-breadcrumb.json') as f:
        breadcrumbs = json.load(f)
        f.close()

    print(len(stops), len(breadcrumbs))

    c = Collector()

    for s in stops:
        c.receive_stop_event(s)

    for b in breadcrumbs:
        c.receive_breadcrumb(b)

    entries = c.integrate()
    with open('2021-02-21-output.json', 'w') as f:
        json.dump(entries, f, indent=4)
    print(len(entries))
