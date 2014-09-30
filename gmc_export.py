#!/usr/bin/env python
# Copyright 2014 Google Inc. All Rights Reserved.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""Exports data from Google Maps Coordinate to a CSV file.

Usage: python gmc_export.py <team-id> <output-file.csv>

If the output-file argument is omitted, CSV data will be written to stdout.

If authentication is needed, your browser will open a page asking you to
grant permission; click "Accept" to proceed.  This script should then
write out a CSV file containing all the job data for your team.

You will need a Google Maps Coordinate Team ID and a client secrets file
in order to use this script.

To find your Team ID:

  - Log into coordinate.google.com.
  - Select a team.
  - Copy the Team ID from the URL (it's the part after the last slash,
        and it looks like "M7jknfvZrnTTqIE2cd6k5g").

To get a client secrets file:

  - Go to https://console.developers.google.com/ and create a project there.
  - Go to APIs & Auth > APIs, scroll down to "Google Maps Coordinate API",
        and click the "OFF" button to turn on the API.
  - Go to APIs & Auth > Consent screen, and fill in the form.
  - Go to APIs & Auth > Credentials, click "Create new Client ID", select
        "Installed application", leave the application type as "Other", and
        click "Create Client ID".
  - Click "Download JSON" to get a client secrets file.  Copy this file to
        "client_secrets.json" in the same directory as this script.
"""

__author__ = 'jlivni@google.com (Josh Livni), kpy@google.com (Ka-Ping Yee)'

import argparse
import csv
import datetime
import httplib2
import logging
import os
import pprint
import sys

# These modules are part of the Google APIs Client Library for Python, which
# you can install with: sudo pip install --upgrade google-api-python-client
import apiclient.discovery
import oauth2client.client
import oauth2client.tools

def authorize(flags, scope, client_secrets_path, credentials_path):
    """Authorizes an HTTP object with the user's credentials.

    Args:
        flags: Command-line flags from argparse.ArgumentParser.parse_args().
        scope: OAuth scope URL.
        client_secret_path: Path to an existing client_secrets.json file.
        credentials_path: Path where the user's credentials are stored; if
            this file doesn't exist yet, the user will be taken through the
            consent flow and then the credentials will be saved here.
    """
    storage = oauth2client.file.Storage(credentials_path)
    credentials = storage.get()
    if not credentials or credentials.invalid:
        flow = oauth2client.client.flow_from_clientsecrets(
            client_secrets_path, scope=scope,
            message=oauth2client.tools.message_if_missing(client_secrets_path))
        credentials = oauth2client.tools.run_flow(flow, storage, flags)
    return credentials.authorize(httplib2.Http())

def get_service(flags, name, version):
    """Sets up access to a Google API.

    Args:
        flags: Command-line flags from argparse.ArgumentParser.parse_args().
        name: The name of the API, e.g. 'coordinate'.
        version: The version of the API, e.g. 'v1'.
    """
    return apiclient.discovery.build(name, version, http=authorize(
        flags,
        'https://www.googleapis.com/auth/' + name,
        os.path.join(os.path.dirname(__file__), 'client_secrets.json'),
        'user_credentials.json'))

class Team:
    def __init__(self, flags, team_id):
        """Data accessor for a Google Maps Coordinate team."""
        self.service = get_service(flags, 'coordinate', 'v1')
        self.team_id = team_id

    def get_custom_fields(self):
        """Returns a dictionary mapping custom field IDs to field names."""
        items = self.service.customFieldDef().list(
            teamId=self.team_id).execute()['items']
        return {int(item['id']): item['name'] for item in items}

    def get_all_jobs(self):
        """Yields a sequence of job dictionaries, including the state and the
        jobChange timestamps but omitting the rest of the jobChange items."""
        jobs = self.service.jobs()
        request = jobs.list(
            teamId=self.team_id,
            fields='items(id,state,jobChange(timestamp)),nextPageToken',
            maxResults=100
        )
        while request:
            response = request.execute()  # fetch one page
            if 'items' not in response:
                break
            for item in response['items']:
                yield item
            request = jobs.list_next(request, response)  # advance to next page

def export_to_csv(team, out, verbose=False):
    """Exports all the jobs for a team to a CSV output stream."""
    if verbose:
        sys.stderr.write('.')
    custom_fields = team.get_custom_fields()
    custom_field_ids, custom_field_names = zip(*sorted(custom_fields.items()))

    writer = csv.writer(out)

    # Write the first row of column headings.
    writer.writerow([
        'Job ID',
        'Last update',
        'Title',
        'Assignee',
        'Progress',
        'Address',
        'Lat',
        'Lon',
        'Contact name',
        'Contact phone',
        'Notes',
    ] + list(custom_field_names))

    if verbose:
        sys.stderr.write('.')
    count = 0
    for job in team.get_all_jobs():
        last_change = max(int(c.get('timestamp') or 0)
                          for c in job.get('jobChange') or [{}])
        dt = datetime.datetime.utcfromtimestamp(last_change/1000)
        timestamp = last_change and dt.strftime('%Y-%m-%d %H:%M:%S UTC') or ''
        state = job.get('state')
        location = state.get('location')
        fields = (state.get('customFields') or {}).get('customField') or {}
        custom = {int(field.get('customFieldId') or 0): field.get('value', '')
                  for field in fields}

        # Write the field values in the same order as the header row.
        writer.writerow([
            job['id'],
            timestamp,
            state.get('title', ''),
            state.get('assignee', ''),
            state.get('progress', ''),
            ' / '.join(location.get('addressLine', '')),
            location.get('lat'),
            location.get('lng'),
            state.get('customerName'),
            state.get('customerPhoneNumber'),
            ' / '.join(state.get('note')),
        ] + [custom.get(id, '') for id in custom_field_ids])
        count += 1

        if verbose and count % 10 == 0:
            sys.stderr.write('.')

    return count

def main(argv):
    # Get the team ID and other oauth2client flags from the command line.
    argparser = argparse.ArgumentParser(
        description=__doc__,
        formatter_class=argparse.RawDescriptionHelpFormatter,
        parents=[oauth2client.tools.argparser])
    argparser.add_argument('team_id', help='Google Maps Coordinate Team ID')
    argparser.add_argument('out_file', help='Output CSV file name', nargs='?',
                           type=argparse.FileType('w'), default=sys.stdout)
    flags = argparser.parse_args(argv[1:])

    logging.basicConfig()  # silence logging warnings

    # Export all the data in CSV format to the given file.
    sys.stderr.write('Working...')
    team = Team(flags, flags.team_id)
    count = export_to_csv(team, flags.out_file, verbose=True)
    flags.out_file.close()
    print >>sys.stderr, '\n%d job%s written to %s.' % (
        count, count != 1 and 's' or '', flags.out_file.name)

if __name__ == '__main__':
    main(sys.argv)
