import itertools
import MySQLdb
import numpy as np
import traceback

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

    def grade_query(self, student_query, grader_query, options=None):
        student_cols, student_rows = self.execute_query(student_query)
        grader_cols, grader_rows = self.execute_query(grader_query)

        Tester = ResultsTester(student_cols, student_rows, grader_cols, grader_rows)
        tests = Tester.default_tests

        print "Grader Results:"
        print self.str_results(grader_cols, grader_rows)
        print "Student Results:"
        print self.str_results(student_cols, student_rows)

        score = Tester.run_tests(tests)
        print "Final Score:", score

class ResultsTester():
    def __init__(self, student_cols, student_rows, grader_cols, grader_rows):
        self.student_cols = student_cols
        self.student_rows = student_rows
        self.grader_cols = grader_cols
        self.grader_rows = grader_rows

        self.default_tests = {
            self.rows_count_test: .1,
            self.cols_count_test: .1,
            self.rows_unsorted_test: .1,
            self.cols_exact_test: .1,
            self.rows_exact_test: .05,
            self.cols_unsorted_test: .05,
            self.rows_sanity_test: .25,
            self.cols_sanity_test: .25
        }
    
    # tests - dictionary of test methods to run, test => points
    def run_tests(self, test_dict):
        assert sum(test_dict.values()) == 1, "Test scores do not add up to 1"
        score = 0

        for test, value in test_dict.iteritems():
            result = test()
            if result:
                score += value
            print "Running %s: %s" % (test.__name__, result)

        return score

    def rows_exact_test(self):
        return (self.student_rows == self.grader_rows)

    def rows_count_test(self):
        return (len(self.student_rows) == len(self.grader_rows))

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
        if (not self.cols_count_test()) or (not self.rows_count_test()):
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

    def rows_sanity_test(self, threshold=.5):
        """Tests if the length of student rows is close to length of grader rows
           Gives points for "reasonable" attempts
        """
        return (abs(1.0*len(self.student_rows) - len(self.grader_rows)) / len(self.grader_rows)) <= threshold

    def cols_exact_test(self):
        return (self.student_cols == self.grader_cols)

    def cols_count_test(self):
        return (len(self.student_cols) == len(self.grader_cols))

    def cols_unsorted_test(self):
        return (sorted(self.student_cols) == sorted(self.grader_cols))

    def cols_sanity_test(self, threshold=.5):
        """Tests if the length of student columns is close to length of grader columns
           Gives points for "reasonable" attempts
        """
        return (abs(1.0*len(self.student_cols) - len(self.grader_cols)) / len(self.grader_cols)) <= threshold

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
