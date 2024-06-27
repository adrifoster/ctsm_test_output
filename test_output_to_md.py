import os
import pandas as pd
import numpy as np
import tabulate
import glob

def get_test_status_data(test):
    """Parses TestStatus text file for an input individual test directory and returns two
      Pandas dataframe with information about phases/results (PASS, FAIL, etc.) as
      well as timing

    Args:
        test (str): full path to individual test directory

    Returns:
        result_df, time_df (pd.Dataframe): Pandas dataframes with test output information
    """
    # test status file - for now use TestStatus (not TestStatus.log)
    test_status_file = os.path.join(test, 'TestStatus')
    test_name = os.path.basename(".".join(test.split('.')[:5]))
    
    results = []
    phases = []
    if os.path.isfile(test_status_file):
        test_status = open(test_status_file, "r")
        lines = test_status.readlines()

        # parse each line
        # WARNING - THIS IS FRAGILE
        for line in lines:
            
            # result is the first word
            result = line.split(' ')[0]

            # phase is third
            phase = line.split(' ')[2].strip('\n')

            # for some phases we have time information
            if phase == 'SHAREDLIB_BUILD':
                lib_time = line.split(' ')[-1].strip("time=").strip("\n")
            elif phase == 'MODEL_BUILD':
                build_time = line.split(' ')[-1].strip("time=").strip("\n")
            elif phase == 'RUN':
                run_time = line.split(' ')[-1].strip("time=").strip("\n")

            results.append(result)
            phases.append(phase)
    else:
          run_time = 0.0
          lib_time = 0.0
          build_time = 0.0
    # turn into pandas dfs and add test name and path
    result_df = pd.DataFrame({'phase': phases, 'result': results} )
    result_df['name'] = test_name
    result_df['path'] = test

    time_df = pd.DataFrame({'name': [test_name], 'lib_time': [lib_time], 'build_time': [build_time],
               'run_time': [run_time]})
    
    return result_df, time_df
  
def get_all_test_data(test_suite_dir):
    """Runs get_test_status_data on all test directories in a top-level directory

    Args:
        test_suite_dir (str): full path to test suite directory

    Returns:
        pd.Dataframes : data frames with all test status and timing information
    """
    
    # find all tests in test suite
    tests = sorted([f for f in os.listdir(test_suite_dir)
                    if os.path.isdir(os.path.join(test_suite_dir, f)) and not f.startswith('sharedlibroot')])
    
    # run through all tests
    all_test_data = []
    all_time_data = []
    for test in tests:
        test_df, time_df = get_test_status_data(os.path.join(test_suite_dir, test))
        all_test_data.append(test_df)
        all_time_data.append(time_df)

    # concat and sort by test name
    result_df = pd.concat(all_test_data)
    result_df = result_df.sort_values(by=['name'])
    
    time_df = pd.concat(all_time_data)
    time_df = time_df.sort_values(by=['name'])
    
    return result_df, time_df
  
def get_test_diffs(cprnc_file_name, test_name):
    """parses a cprnc.out file the normalized and absolute differences for all 
       non-bit-for-bit variables

    Args:
        cprnc_file_name (str): full path to cprnc.out file
        test_name (str): name of test - for outputing

    Returns:
        pd.Dataframe: pandas dataframe with variable differences for this file
    """
    cprnc_file = open(cprnc_file_name, "r")
    lines = cprnc_file.readlines()
    
    variables = []
    diff = []
    normalized_diff = []
    # parse each line
    # WARNING - THIS IS FRAGILE
    for line in lines:
        if 'RMS' in line:
            # I hate this section...
            line_split = line.split('NORMALIZED')
            var = line_split[0].strip('RMS ').split(' ')[0]
            norm_diff = float(line_split[-1])
            var_diff = float(line_split[0].strip('RMS ').strip(var).strip())
            
            variables.append(var)
            normalized_diff.append(norm_diff)
            diff.append(var_diff)
            
    df = pd.DataFrame({'variable': variables, 'diff': diff, 'normalized_diff': normalized_diff})
    df['test'] = test_name
    return df
  
def get_all_test_diffs(test_list):
    """Runs get_test_diffs on all tests in an input test list

    Args:
        test_list ([str]): list of full paths to tests

    Returns:
        pd.Dataframe: pandas dataframe with all variable difference information
    """
    
    all_dfs = []
    for test in test_list:
        test_name = os.path.basename(".".join(test.split('.')[:5]))
        files = glob.glob(f'{os.path.join(test, "run")}/*.nc.cprnc.out')
        for file in files:
            df = get_test_diffs(file, test_name)
            all_dfs.append(df)
    out = pd.concat(all_dfs)
    
    return out
  
def encode_string(val, color='black'):
    """encodes a string for writing to html, optionally add color

    Args:
        val (str): string to encode
        color (str, optional): color. Defaults to 'black'.

    Returns:
        str: encoded string
    """
    return f'<span style="color:{color}">{val}</span>\n'.encode()
  
def main(test_dir):
    """main script that parses test output

    Args:
        test_dir (str): full path to top-level test suite directory
    """
    
    # get all testing data
    all_test_data, all_time_data = get_all_test_data(test_dir)
    
    # summarize data - get counts for each phase and result across tests
    # to do - get "expected fails" and do something with that
    summary_dat = all_test_data.pivot_table(index='result', columns='phase', aggfunc='size')
    summary_dat = summary_dat.fillna(0)
    
    # total tests run
    num_tests = len(np.unique(all_test_data['name']))
    
    # list of phases
    phases = np.unique(all_test_data['phase'])
    
    # grab just baseline tests that FAILED
    baseline_data = all_test_data[all_test_data.phase == 'BASELINE']
    baseline_fails = np.unique(baseline_data[baseline_data.result == 'FAIL'].path)
    
    # create difference dataframe from list of baseline fails
    diff_df = get_all_test_diffs(baseline_fails)
    
    # sometimes the differences are infinity? replace with nan
    diff_df.replace([np.inf, -np.inf], np.nan, inplace=True)
    
    # get maximum differences for each variable/test
    maxes = diff_df.pivot_table(index='test', columns='variable', aggfunc='max')
    
    # subset only tests that did not PASS
    # to do - get "expected fails" and then get rid of those as well
    non_passing = all_test_data[all_test_data.result != 'PASS']
    non_passing = non_passing.sort_values(by=['phase', 'result'])
    
    # make a markdown file with these results
    with open('test_results.md', 'bw') as f:
        f.write(f'# Test Results for {test_dir}\n'.encode())
        
        f.write('## Testing Summary\n'.encode())
        
        f.write(f'A total of {num_tests} tests were run \n'.encode())
        f.write(' \n'.encode())
        
        for phase in phases:
            result = summary_dat[phase]
            if result['FAIL'] > 0.0:
                f.write(encode_string(f'{int(result["FAIL"])} {phase} tests failed.', 'red'))
                f.write(' \n'.encode())
            if result['PEND'] > 0.0:
                f.write(encode_string(f'{int(result["PEND"])} {phase} tests are pending.', 'orange'))
                f.write(' \n'.encode())

        f.write('## All Non-Passing Tests\n'.encode())
        
        f.write(non_passing.to_markdown().encode())
        f.write(' \n'.encode())

        f.write('## Difference Data\n'.encode())
        
        f.write(maxes.to_markdown().encode())
        f.write(' \n'.encode())

if __name__ == '__main__':
    main('/glade/derecho/scratch/afoster/tests_0618-145127de')