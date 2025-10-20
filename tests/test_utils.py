import unittest
import time
import os
from unittest.mock import patch
import sys 
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from utils import timed_call

# --- Helper function for testing ---
def dummy_function(a, b, delay=0.01):
    """A function that returns the sum of two numbers after a small delay."""
    time.sleep(delay)
    return a + b

class TestUtils(unittest.TestCase):
    """
    Unit tests for the utility functions in utils.py.
    """

    def test_timed_call_returns_correct_result(self):
        """
        Tests that timed_call correctly returns the result of the wrapped function.
        """
        expected_result = 50
        
        # We set delay=0 for this test to ensure focus is on the return value
        result = timed_call(dummy_function, 20, 30, delay=0)
        
        self.assertEqual(result, expected_result, "timed_call did not return the expected result.")

    @patch('builtins.print')
    def test_timed_call_prints_timing_info(self, mock_print):
        """
        Tests that timed_call prints a message containing the function name and time.
        
        We use @patch('builtins.print') to intercept the output and inspect it.
        """
        # Run the function with a short delay to ensure a measurable time is printed
        timed_call(dummy_function, 1, 1, delay=0.1)
        
        # 1. Check that print was called at least once
        mock_print.assert_called()
        
        # 2. Get the printed string (it should be the first argument of the call)
        printed_output = mock_print.call_args[0][0]
        
        # 3. Check for the function name in the output
        self.assertIn("Time taken for dummy_function:", printed_output, 
                      "The timing message does not mention the function name.")

        # 4. Check the format: it should match 'Time taken for <func>: XX.XX seconds'
        # The regex ensures the time value is a float with 2 decimal places.
        self.assertRegex(printed_output, r"Time taken for dummy_function: \d+\.\d{2} seconds",
                         "The format of the time measurement is incorrect.")
        
    def test_timed_call_executes_function(self):
        """
        A simple check to ensure the wrapped function is actually executed, 
        by verifying the total elapsed time is greater than the enforced sleep.
        """
        delay_sec = 0.05
        start_time = time.time()
        
        # Mock print to avoid output, focusing only on timing measurement
        with patch('builtins.print'):
            timed_call(dummy_function, 1, 1, delay=delay_sec)
            
        end_time = time.time()
        elapsed_time = end_time - start_time
        
        # The execution time must be at least the enforced delay
        self.assertTrue(elapsed_time >= delay_sec, "The function execution was not properly timed or executed.")
