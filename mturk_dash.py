import json
from typing import List

import jsonlines
import streamlit as st
import pandas as pd
import boto3
import io
from xml.dom.minidom import parseString


def get_mturk_client(env):
    is_sandbox = env.lower() == 'sandbox'
    if is_sandbox:
        endpoint_url = 'https://mturk-requester-sandbox.us-east-1.amazonaws.com'
    else:
        endpoint_url = 'https://mturk-requester.us-east-1.amazonaws.com'

    print("Using sandobx: ", is_sandbox)
    mturk_client = boto3.client('mturk', endpoint_url=endpoint_url, region_name='us-east-1')
    return mturk_client


def paginate(operation, result_fn, max_results: int, **kwargs):
    results = []
    resp = operation(MaxResults=100, **kwargs)
    local_results = result_fn(resp)
    results.extend(local_results)
    next_token = resp.get('NextToken')
    # with tqdm() as pbar:
    my_bar = st.progress(0)
    while local_results and next_token:
        if max_results is not None and len(results) >= max_results:
            break
        resp = operation(MaxResults=100, NextToken=next_token, **kwargs)
        local_results = result_fn(resp)
        next_token = resp.get('NextToken')
        results.extend(local_results)
        percent_complete = min(len(results)/max_results, 1.0)
        my_bar.progress(percent_complete)
    my_bar.progress(1.0)
    return results


@st.cache(suppress_st_warning=True)
def get_all_hits(mturk_env: str, max_results: int):
    mturk = get_mturk_client(mturk_env)
    hits = paginate(mturk.list_hits, lambda resp: resp['HITs'], max_results=max_results)
    return hits


def to_hit_df(hits):
    df = pd.DataFrame(hits)
    cols = ['HITId', 'HITTypeId', 'Title', 'HITStatus',  'Expiration', 'HITReviewStatus',
            'NumberOfAssignmentsPending', 'NumberOfAssignmentsAvailable', 'NumberOfAssignmentsCompleted']
    df = df[cols].copy()
    df.rename(columns={
        'HITId': 'hit_id',
        'HITTypeId': 'hit_type_id',
        "Title": 'title',
        "HITStatus": "hit_status",
        "HITReviewStatus": 'review_status',
        "NumberOfAssignmentsPending": "n_assigns_pending",
        "NumberOfAssignmentsAvailable": "n_assigns_available",
        "NumberOfAssignmentsCompleted": 'n_assigns_completed'
    }, inplace=True)
    return df


def to_hit_summary(hit_df):
    df2 = hit_df.groupby(['hit_type_id', 'title', 'hit_status', 'review_status']).hit_id.nunique().rename("n_hits").reset_index()
    cols = ['title', 'hit_status', 'review_status', 'n_hits', 'hit_type_id']
    df2 = df2[cols].copy()
    df2.sort_values(cols, inplace=True)
    return df2


def parse_answer(assign):
    answer_xml = parseString(assign['Answer'])
    # //QuestionFormAnswers/Answer/FreeText
    free_text_nodes = answer_xml.getElementsByTagName('FreeText')
    assert(len(free_text_nodes) <= 1)
    free_text = free_text_nodes[0]
    answer = " ".join(t.nodeValue for t in free_text.childNodes if t.nodeType == t.TEXT_NODE)
    result = json.loads(answer)
    return result


def parse_assignment(assign):
    accept_time = assign['AcceptTime']
    submit_time = assign['SubmitTime']
    seconds = (submit_time - accept_time).seconds
    result = {
        'hit_id': assign['HITId'],
        'assign_id': assign['AssignmentId'],
        'worker_id': assign['WorkerId'],
        'assign_status': assign['AssignmentStatus'],
        'seconds': seconds
    }
    try:
        more = parse_answer(assign)
        result.update(more)
    except Exception as e:
        result['parsing_error']: e
    return result


def get_account_balance(mturk_env):
    mturk = get_mturk_client(mturk_env)
    resp = mturk.get_account_balance()
    return resp["AvailableBalance"]


def get_reviewable_hits(mturk_client, hit_type_id) -> List[str]:
    rev_hits = paginate(mturk_client.list_reviewable_hits,
                        lambda resp: resp.get('HITs', []),
                        max_results=10000,
                        HITTypeId=hit_type_id)
    hit_ids = [h['HITId'] for h in rev_hits]
    return hit_ids


def retrieve_assignments(mturk_env, hit_type_id, hit_ids):
    mturk_client = get_mturk_client(mturk_env)
    assignments_for_review = []
    progress_bar = st.progress(0)
    for idx, hit_id in enumerate(hit_ids):
        resp = mturk_client.list_assignments_for_hit(HITId=hit_id, MaxResults=100)
        assignments_for_hit = resp.get('Assignments', [])
        for assign in assignments_for_hit:
            parsed_assignment = parse_assignment(assign)
            parsed_assignment['hit_type_id'] = hit_type_id
            assignments_for_review.append(parsed_assignment)
        progress_bar.progress(idx/len(hit_ids))
    progress_bar.progress(1.0)
    return assignments_for_review


def review_assignments(mturk_env, hit_type_id):
    mturk_client = get_mturk_client(mturk_env)

    hit_ids = get_reviewable_hits(mturk_client, hit_type_id)

    assignments_for_review = retrieve_assignments(mturk_client, hit_type_id, hit_ids)
    return assignments_for_review


mturk_env = st.selectbox("Please select the MTurk environment: ", ['Sandbox', 'Production'])
st.write(f"Using MTurk in {mturk_env} mode")

st.write("Available balance: ",  get_account_balance(mturk_env))

max_hits = st.slider(f"Max number of HITs to retrieve from {mturk_env}? ", 100, 20000)

hits = get_all_hits(mturk_env, max_hits)
all_hits_df = to_hit_df(hits)
summary_hits_df = to_hit_summary(all_hits_df)

st.subheader("HIT Types")
st.table(summary_hits_df)

hit_types = all_hits_df[['hit_type_id', 'title']].drop_duplicates().to_dict(orient='records')
hit_type = st.selectbox("Please select the HIT Type for a drill-down of the assignments",
                        hit_types,
                        format_func=lambda a: f"{a['title']} - {a['hit_type_id'][:3]}...{a['hit_type_id'][-3:]}")
selected_hit_type_id = hit_type['hit_type_id']

selected_hits_df = all_hits_df[all_hits_df.hit_type_id == selected_hit_type_id].copy()
n_available = selected_hits_df.n_assigns_available.sum()
n_pending = selected_hits_df.n_assigns_pending.sum()
n_completed = selected_hits_df.n_assigns_completed.sum()

st.subheader("Completed Assignments")

metric_cols = st.columns(3)
with metric_cols[0]:
    st.metric("Assignments available for workers: ", n_available)
with metric_cols[1]:
    st.metric("Assignments in progress: ", n_pending)
with metric_cols[2]:
    st.metric("Assignments reviewed (approved or rejected) : ", n_completed)

selected_hit_ids = selected_hits_df.hit_id.tolist()
with st.spinner(text="Downloading assignments..."):
    assignments = retrieve_assignments(mturk_env, selected_hit_type_id, selected_hit_ids)

if not assignments:
    st.write("No assignments for review for this Hit Type")
else:
    assign_df = pd.DataFrame(assignments)[['worker_id', 'hit_id', 'assign_status']]
    worker_df = assign_df.groupby('worker_id').hit_id.nunique().rename('Assignments completed').reset_index()
    st.table(worker_df)

    n_pending_review = assign_df[assign_df.assign_status == "Submitted"].shape[0]
    metric_cols = st.columns(2)
    with metric_cols[0]:
        st.metric("Assignments pending for review: ", n_pending_review)
    with metric_cols[1]:
        st.metric("Total assignments completed: ", assign_df.shape[0])

mem_buffer = io.StringIO()
with jsonlines.Writer(mem_buffer) as file_out:
    file_out.write_all(assignments)
mem_buffer.seek(0)
content = mem_buffer.read()
st.download_button('Download Assignments',
                   data=content,
                   file_name="assignments.jsonl",
                   mime='text/plain')