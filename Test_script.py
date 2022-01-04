import unittest
from data_result_output import *


class my_test(unittest.TestCase):    
    
    def test_1(self):
        self.assertEqual(coin_requests_s3_result, coin_requests_s3_result, f"\n\n>>> Test coin_data_result_s3 result should be:{coin_requests_s3_result}")
    
    def test_2(self):
        self.assertEqual((100,20), exchange_data_s3_result, f"\n\n>>> Test exchange_data_result_s3 result should be:{exchange_data_s3_result}")

    def test_3(self):
        self.assertEqual((100,20), historical_s3_result, f"\n\n>>> Test historical_data_result_s3 result should be:{historical_s3_result}")
    
    def test_4(self):
        self.assertEqual((100,20), top_coins_s3_result, f"\n\n>>> Test top_coins_data_result_s3 result should be:{top_coins_s3_result}")
        



suite = unittest.TestLoader().loadTestsFromTestCase(my_test)
runner = unittest.TextTestRunner(verbosity=3)
runner.run(suite)
print("\n\nDone test for Data shape at local sources\n\n".upper())


