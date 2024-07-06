import csv
import io
import unittest

from SOS import splitDays

CSVHeader = "Rest Nbr|Rest Nm|Cal Dt|Hour Nbr|DT Avg Order Time|DT Avg Line Time|DT Avg Serve Time|DT Avg Total Time|DT Orders Over 5 Min|DT Orders Over 7 Min|DT Orders Over 10 Min|DT Order Qty|Tablet Avg Order Time|Tablet Avg Line Time|Tablet Avg Serve Time|Tablet Avg Total Time|Tablet Orders Over 5 Min|Tablet Orders Over 7 Min|Tablet Orders Over 10 Min|Tablet Order Qty"
CSVLine = "612|Bemidji, MN - Paul Bunyan Dr NW|2024-06-24|21|57.3|107.2|88.7|253.1|6|1|0|16|0.0|0.0|0.0|0.0|0|0|0|0"
CSVLine2 = "493|Fort Myers, FL - University Plaza Dr|2024-06-17|15|42.4|33.4|107.5|183.3|0|0|0|14|0.0|0.0|0.0|0.0|0|0|0|0"

def makeReader(CSVLines):
    return csv.DictReader(io.StringIO('\n'.join([CSVHeader, *CSVLines])), delimiter='|')

class TestSplitDays(unittest.TestCase):

    def testEmptyInput(self):
        reader = makeReader([])
        self.assertEqual({}, splitDays(reader),
            "Empty input should result in an empty dict.")

    def testEntryForDateHasCorrectValues(self):
        reader = makeReader([CSVLine])
        self.assertEqual(
            [["2024-06-24", "21", "57.3", "107.2", "88.7", "253.1", "6", "1", "0", "16"]],
            splitDays(reader)[612]["2024-06-24"],
            "The entry for 612/2024-06-24 should be the right one.")

        reader = makeReader([CSVLine2])
        self.assertEqual(
            [["2024-06-17", "15", "42.4", "33.4", "107.5", "183.3", "0", "0", "0", "14"]],
            splitDays(reader)[493]["2024-06-17"],
            "The entry for 612/2024-06-24 should be the right one.")

    def testTwoRestaurantsBothKeysPresent(self):
        reader = makeReader([CSVLine, CSVLine2])
        self.assertCountEqual([612, 493], splitDays(reader).keys(),
            "When two restaurants are in the CSV, they should compound.")

    def testSameRestaurantTwiceSameDate(self):
        reader = makeReader([CSVLine, CSVLine])
        expectedRow = ["2024-06-24", "21", "57.3", "107.2", "88.7", "253.1", "6", "1", "0", "16"]
        self.assertEqual([expectedRow, expectedRow], splitDays(reader)[612]["2024-06-24"],
            "When two restaurants with the same date are in the CSV, " \
            "then the entries should compound under that date.")

if __name__ == "__main__":
    unittest.main()
