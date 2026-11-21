# farmbuster

catching stream farms in real time. bots pump a track with fake plays,
this thing notices and raises an alert.

## how it works

producer.py → kafka → spark (consumer.py) → kafka → alerts_viewer.py

- `producer.py` fakes regular listeners plus a bot farm (~5% of traffic) pumping one track
- `consumer.py` counts plays per track in 30 sec windows and looks for anything weird
- `alerts_viewer.py` prints the alerts

## how it catches bots

first version was "too many plays = fraud". didn't work great:
a real hit gets tons of plays too, and a careful farm just stays under the limit.

but bots have a tell. a play counts as a stream after ~30 seconds,
so bots listen for 31-35 sec and bounce. real people don't really do that.

so now it's: 6+ plays in a window and 70%+ of them last 30-36 sec.

tested both on 2000 simulated windows (`python evaluate_rules.py --windows 2000`):

- old rule (>10 plays): 65% of attacks caught, 5 false alarms
- new rule: 98% caught, 0 false alarms

## run it

needs docker, python and java 17 (spark wants it)

    docker compose up -d
    python3 -m venv venv && source venv/bin/activate
    pip install -r requirements.txt

    python producer.py        # terminal 1
    python consumer.py        # terminal 2
    python alerts_viewer.py   # terminal 3

give it a minute and you'll get something like

    [ALERT] track_bot_farm_victim_99  plays=13  users=13  short_plays=100%

## not great yet

- the bots are pretty dumb. real farms randomize everything
- thresholds are fixed. comparing each track to its own normal would be smarter
- alerts just sit in kafka. want to store them somewhere and build a dashboard
