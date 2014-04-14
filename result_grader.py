import itertools
import MySQLdb
import numpy as np

class InvalidQuery(Exception):
    pass

class Grader:
    """Executes student and grader MySQL queries, and compares result sets"""
    def __init__(self, database, host, user, passwd, port=3306, *args, **kwargs):
        try:
            self.db = MySQLdb.connect(host, user, passwd, database, port)
        except MySQLdb.OperationalError as e:
            raise Exception("Cannot connect to MySQL")

    def execute_query(self, stmt):
        cursor = self.db.cursor()
        try:
            cursor.execute(stmt)
            rows = cursor.fetchall()
            cols = [str(col[0]) for col in cursor.description]
        except (MySQLdb.OperationalError, MySQLdb.Warning, MySQLdb.Error) as e:
            msg = e.args[1]
            code = e.args[0]
            raise InvalidQuery("MySQL Error {}: {}".format(code, msg))
        return cols, rows

    def str_results(self, cols, rows):
        text = ", ".join(map(str, cols)) + "\n"
        text += "-" * 30 + "\n"
        for row in rows:
            text += ", ".join(map(str, row)) + "\n"
        return text

    def grade_query(self, student_query, grader_query, print_results=False, options=None):
        student_cols, student_rows = self.execute_query(student_query)
        grader_cols, grader_rows = self.execute_query(grader_query)
        
        if print_results:
            print "Grader Results:"
            print self.str_results(grader_cols, grader_rows)
            print "Student Results:"
            print self.str_results(student_cols, student_rows)

        Tester = ResultsTester(student_cols, student_rows, grader_cols, grader_rows)

        score, hints = Tester.run_rubric_tests()
        print "Final Score:", score
        if hints:
            print "Hints:", " ".join(hints)

class ResultsTester():
    def __init__(self, student_cols, student_rows, grader_cols, grader_rows):
        self.student_cols = student_cols
        self.student_rows = student_rows
        self.grader_cols = grader_cols
        self.grader_rows = grader_rows

        self.default_tests = {
            self.rows_unsorted_test: .1,
            self.cols_unsorted_test: .05,
            self.rows_exact_test: .05,
            self.cols_exact_test: .1,
            self.rows_count_linear_test: 0,
            self.cols_count_linear_test: 0,
            self.rows_count_close_test: .25,
            self.cols_count_close_test: .25,
            self.rows_count_exact_test: .1,
            self.cols_count_exact_test: .1,
            self.rows_unsorted_guess: 0,
        }
    
    # tests - dictionary of test methods to run, test => points
    def run_tests(self, test_dict):
        # Can't do straight equality check because of float imprecision
        assert abs(sum(test_dict.values()) - 1.0) < .01, "Test scores do not add up to 1"
        score = 0

        failed_tests = []
        passed_tests = []
        for test, value in test_dict.iteritems():
            result = test()
            points = float(result) * value
            score += points 
            test_str = "%s: %s (%s of %s points)" % (test.__name__, result, points, value)
            if float(result) == 1.0:
                passed_tests.append(test_str)
            else:
                failed_tests.append(test_str)

        print "Passed Tests:"
        for test in passed_tests:
            print test

        print "\nFailed Tests:"
        for test in failed_tests:
            print test

        return score

    def run_rubric_tests(self):
        """Instead of using a weighted average to calculate a detailed score, use a "rubric" approach
        Logical groupings of tests determine a score bucket for the student's query
        """
        rows_tests = [self.rows_count_close_test, self.rows_count_exact_test, self.rows_unsorted_guess, self.rows_exact_test]
        cols_tests = [self.cols_count_close_test, self.cols_count_exact_test, self.cols_unsorted_test, self.cols_exact_test]
        results = {}
        # TODO: implement keyword_test
        results["keyword_test"] = True
        # TODO: Don't run unneccesary tests, clearer test logic
        for test in rows_tests + cols_tests:
            result = test()
            results[test] = result

        if results[self.rows_exact_test] and results[self.cols_exact_test] and results["keyword_test"]:
            # "Perfect"
            score = 1.0
        elif results[self.rows_unsorted_guess] and results["keyword_test"]:
            # "Really Close"
            score = .8
        elif ((results[self.cols_unsorted_test] and results[self.rows_count_close_test]) or (results[self.cols_count_exact_test] and results[self.rows_count_exact_test]) or (results[self.cols_exact_test])) and results["keyword_test"]:
            # "Nice Try"
            score = .6
        elif (results[self.rows_count_close_test] and results[self.cols_count_close_test]):
            # "Decent Attempt"
            score = .4
        else:
            # "Fail"
            score = .0
        
        # TODO: Make this sane
        hints = []
        if not results["keyword_test"]:
            hints.appned("Missing SQL Keyword")

        if not results[self.rows_count_exact_test]:
            if len(self.student_rows) > len(self.grader_rows):
                hints.append("Too many rows.")
            else:
                hints.append("Too few rows.")
        
        if not results[self.cols_count_exact_test]:
            if len(self.student_cols) > len(self.grader_cols):
                hints.append("Too many columns.")
            else:
                hints.append("Too few columns.")
        elif not results[self.cols_unsorted_test]:
            hints.append("Columns are named incorrectly.")
        elif not results[self.cols_exact_test]:
            hints.append("Columns are out of order.")
        elif results[self.rows_unsorted_guess] and not results[self.rows_exact_test]:
            hints.append("Rows are out of order.")
        elif results[self.rows_count_exact_test] and not results[self.rows_unsorted_guess]:
            hints.append("Incorrect column calculation")

        return score, hints

    def rows_exact_test(self):
        return (self.student_rows == self.grader_rows)

    def rows_count_linear_test(self):
        return max(1 - abs(1 - 1.0 * len(self.student_rows) / len(self.grader_rows)), 0)

    def rows_count_exact_test(self):
        return (len(self.student_rows) == len(self.grader_rows))

    def rows_count_close_test(self, threshold=.5):
        """Tests if the length of student rows is close to length of grader rows
           Gives points for "reasonable" attempts
        """
        return (abs(1.0 * len(self.student_rows) - len(self.grader_rows)) / len(self.grader_rows)) <= threshold

    def rows_unsorted_guess(self):
        """
        A quick guess of whether rows_unsorted_test will pass
        First each row is indiviudally sorted, then the list of rows is sorted
        Can return false passes (incorrectly guess that unsorted data matches)
        TODO: Optimize sorts using numpy 
        TODO: Improve accuracy by doing the same sorts on columns of data 
        """
        sorted_grader_rows = sorted(sorted(row) for row in self.grader_rows)
        sorted_student_rows = sorted(sorted(row) for row in self.student_rows)
        
        return (sorted_grader_rows == sorted_student_rows)

    def rows_unsorted_test(self):
        """Tests if student and grader rows match, ignoring column and row order
        Can't rely on columns being in same order, or having same names
        This is equivalent to checking if the result matrices are equivalent by row/column permutation
        Not easy: http://math.stackexchange.com/questions/692605/how-to-tell-if-two-matrices-are-equal-up-to-a-permutation
        TODO: Overthinking? Better implementation? Simplifying assumptions?
        TODO: Write lots of tests for this scary thing
        """

        def sort_array_allcols(array):
            """Sorts numpy array rows, using all columns in order (row[0], row[1], ...) as a sort key
            Adapted from http://stackoverflow.com/questions/8153540/sort-a-numpy-array-like-a-table
            """
            return array[np.lexsort([array[:,i] for i in range(array.shape[1])])]

        # Some quick test to avoid unnecessarily complex checks
        # TODO: Add more shortcuts (e.g. column/row "sum" checking)

        # If row or col counts don't match, fail immediately
        if (not self.cols_count_exact_test()) or (not self.rows_count_exact_test()):
            return False
        
        if self.rows_exact_test():
            return True

        # Turn rows into 2D numpy arrays
        grader_rows_array = np.array(self.grader_rows)
        student_rows_array = np.array(self.student_rows)

        # Sort grader array rows using orignal column order
        sorted_grader_rows = sort_array_allcols(grader_rows_array)

        # Check if any valid sort of student_rows matches sorted_grader_rows (valid sort == allow column and row swapping)
        # Rearrange columns, then sort rows by new column order
        # Repeat for every possible arrangement of columns (permutations)
        permutations = itertools.permutations(range(len(self.student_cols)))
        for permutation in permutations:
            i = np.array(permutation)

            # Swap columns using permutation (from http://stackoverflow.com/questions/20265229/rearrange-columns-of-numpy-2d-array)
            swappedcols_student_rows = student_rows_array[:, i]

            # Sort rows by new column order
            sorted_student_rows = sort_array_allcols(swappedcols_student_rows)
            
            # Check for match
            if np.all(sorted_student_rows == sorted_grader_rows):
                return True

        # No possible sorts match - test fails
        return False

    def cols_exact_test(self):
        return (self.student_cols == self.grader_cols)

    def cols_count_linear_test(self):
        return max(1 - abs(1 - 1.0 * len(self.student_cols) / len(self.grader_cols)), 0)

    def cols_count_exact_test(self):
        return (len(self.student_cols) == len(self.grader_cols))

    def cols_count_close_test(self, threshold=.5):
        """Tests if the length of student columns is close to length of grader columns
           Gives points for "reasonable" attempts
        """
        return (abs(1.0 * len(self.student_cols) - len(self.grader_cols)) / len(self.grader_cols)) <= threshold

    def cols_unsorted_test(self):
        return (sorted(self.student_cols) == sorted(self.grader_cols))

if __name__=="__main__":
    Grader = Grader("lahman", "localhost", "root", "")

    # Grader query is defined once
    grader_query = raw_input("Enter Grader Query: ")

    # Interactive testing of student queries against grader query
    while True:
        student_query = raw_input("Enter Student Query: ")

        try:
            Grader.grade_query(student_query, grader_query)
        # For MySQL query errors, print error message and continue
        except InvalidQuery as e:
            print e
            continue
