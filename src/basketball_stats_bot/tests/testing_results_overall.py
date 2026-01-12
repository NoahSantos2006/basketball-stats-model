import json
from datetime import date, datetime, timedelta
import os
import sys
import re

from basketball_stats_bot.config import load_config

if __name__ == "__main__":

    config = load_config()
    
    curr_date_str = '2025-12-01'
    end_date_str = '2025-12-26'
    curr_date = datetime.strptime(curr_date_str, "%Y-%m-%d").date()
    end_date = datetime.strptime(end_date_str, "%Y-%m-%d").date()

    scoring_dict = {}

    while curr_date <= end_date:

        curr_date_testing_result_path = os.path.join(config.TESTING_RESULTS_PATH, f"{str(curr_date)}_grade.txt")

        if not os.path.isfile(curr_date_testing_result_path):

            print(f"Couldn't find a testing result for {str(curr_date)}")
            curr_date += timedelta(days=1)
            continue

        with open(curr_date_testing_result_path, "r") as f:

            content = f.read()

        content = content.split("\n\n\n")

        for scores in content:
            
            splitted = scores.split("\n\n")

            type_of_scoring = splitted[0][-9:]

            if type_of_scoring not in scoring_dict:

                scoring_dict[type_of_scoring] = {

                    'net_money': 0,
                    'number_of_bets': 0,
                    'number_of_props': 0,
                    'number_of_props_hit': 0,
                    'top_10_percentage': 0,

                }

            if len(splitted) < 2:

                continue

            # splitted[1] is the sentence "In 1 props you went 100%"
            top_10_percentage = splitted[1]

            # finds all numbers using regex
            top_10_numbers = re.findall(r"\d+(?:\.\d+)?", top_10_percentage)

            current_number_of_props = int(top_10_numbers[0])

            # finds the number of props that you can bet if you were to only do two mans
            number_of_bets = int(top_10_numbers[0]) // 2

            # adds that to the scoring_dict
            scoring_dict[type_of_scoring]['number_of_props'] += current_number_of_props

            # you can't place a bet with 0 or 1 prop so we don't do anything except add to the total percentage
            if number_of_bets == 0:
                
                # the second number in top_10_numbers is the percentage as "100.00"
                percentage_hit = float(top_10_numbers[1])

                # finds the number of props that hit
                number_of_props_hit = percentage_hit / 100 * current_number_of_props
                
                # adds that to the scoring_dict
                scoring_dict[type_of_scoring]['number_of_props_hit'] += number_of_props_hit

                scoring_dict[type_of_scoring]['top_10_percentage'] = scoring_dict[type_of_scoring]['number_of_props_hit'] / scoring_dict[type_of_scoring]['number_of_props'] * 100
                
                continue
                
            else:
                
                # number of bets as in two mans
                scoring_dict[type_of_scoring]['number_of_bets'] += number_of_bets

                percentage_hit = float(top_10_numbers[1])

                number_of_props_hit = percentage_hit / 100 * current_number_of_props

                scoring_dict[type_of_scoring]['number_of_props_hit'] += number_of_props_hit
                
                money_put_down = 0
                money_won = 0

                for i in range(number_of_bets):

                    money_put_down += 5
                
                number_of_props_that_missed = int(current_number_of_props - number_of_props_hit)

                number_of_bets_that_hit = number_of_bets - number_of_props_that_missed

                if current_number_of_props % 2 == 1:

                    if number_of_props_that_missed != 0:

                        number_of_bets_that_hit -= 1
                    
                    if number_of_props_that_missed == 0:

                        money_won += 10

                for i in range(number_of_bets_that_hit):

                    money_won += 15
                
                scoring_dict[type_of_scoring]['net_money'] += money_won - money_put_down

                scoring_dict[type_of_scoring]['top_10_percentage'] = scoring_dict[type_of_scoring]['number_of_props_hit'] / scoring_dict[type_of_scoring]['number_of_props'] * 100

        
        curr_date += timedelta(days=1)
    
    print(scoring_dict)
        