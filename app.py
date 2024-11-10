import streamlit as st
from datasets import load_dataset
import datetime
from pbs_data import PBSPublicDataAPIClient
import os
from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.triggers.interval import IntervalTrigger
import atexit

HF_TOKEN = os.environ.get("HF_TOKEN")
DATASET_NAME = "cmcmaster/rheumatology-biologics-dataset"
UPDATE_INTERVAL = 1

@st.cache_data
def load_data():
    try:
        dataset = load_dataset(DATASET_NAME, split="train")
        
        # Create sets for dropdown options
        drugs = set(dataset['drug'])
        brands = set(dataset['brand'])
        formulations = set(dataset['formulation'])
        indications = set(dataset['indication'])
        treatment_phases = set(dataset['treatment_phase'])
        hospital_types = set(dataset['hospital_type'])

        return {
            'combinations': dataset,
            'drugs': sorted(drugs),
            'brands': sorted(brands),
            'formulations': sorted(formulations),
            'indications': sorted(indications),
            'treatment_phases': sorted(treatment_phases),
            'hospital_types': sorted(hospital_types)
        }
    except Exception as e:
        st.error(f"An error occurred while loading data: {str(e)}")
        return {
            'combinations': [],
            'drugs': [],
            'brands': [],
            'formulations': [],
            'indications': [],
            'treatment_phases': [],
            'hospital_types': []
        }

@st.cache_data
def get_valid_options(_combinations, current_selections):
    """Get valid options for each field based on current selections"""
    # Apply filters sequentially based on the order of fields
    filtered = _combinations
    
    # Order of fields matches sequential_search
    fields_order = ['indication', 'drug', 'brand', 'formulation', 'treatment_phase', 'hospital_type']
    
    # Build up filters one by one
    current_filters = {}
    for field in fields_order:
        if current_selections[field]:
            current_filters[field] = current_selections[field]
            filtered = filtered.filter(
                lambda x: all(
                    x[f] == v for f, v in current_filters.items()
                )
            )
    
    # Convert filtered Dataset to list of dictionaries for easier processing
    filtered_list = [dict(item) for item in filtered]
    
    # Use lowercase keys consistently
    options = {
        'drugs': sorted({item['drug'] for item in filtered_list}),
        'brands': sorted({item['brand'] for item in filtered_list}),
        'formulations': sorted({item['formulation'] for item in filtered_list}),
        'indications': sorted({item['indication'] for item in filtered_list}),
        'treatment_phases': sorted({item['treatment_phase'] for item in filtered_list}),
        'hospital_types': sorted({item['hospital_type'] for item in filtered_list})
    }
    
    return options

def search_biologics(combinations, selections):
    results = combinations.filter(
        lambda x: (not selections['drug'] or x['drug'] == selections['drug']) and
                 (not selections['brand'] or x['brand'] == selections['brand']) and
                 (not selections['formulation'] or x['formulation'] == selections['formulation']) and
                 (not selections['indication'] or x['indication'] == selections['indication']) and
                 (not selections['treatment_phase'] or x['treatment_phase'] == selections['treatment_phase']) and
                 (not selections['hospital_type'] or x['hospital_type'] == selections['hospital_type'])
    )
    
    if len(results) == 0:
        st.warning("No results found.")
        return
    
    for item in results:
        st.markdown(f"""
### {item['drug']} ({item['brand']})

* **PBS Code:** [{item['pbs_code']}](https://www.pbs.gov.au/medicine/item/{item['pbs_code']})
* **Formulation:** {item['formulation']}
* **Indication:** {item['indication']}
* **Treatment Phase:** {item['treatment_phase']}
* **Streamlined Code:** {item['streamlined_code'] or 'N/A'}
* **Authority Method:** {item['authority_method']}
* **Online Application:** {'Yes' if item['online_application'] else 'No'}
* **Hospital Type:** {item['hospital_type']}
* **Schedule:** {item['schedule_month']} {item['schedule_year']}

---
""")

def update_data():
    if datetime.datetime.now().day == 1:
        with st.spinner("Updating data..."):
            client = PBSPublicDataAPIClient("2384af7c667342ceb5a736fe29f1dc6b", rate_limit=0.2)
            try:
                data = client.fetch_rheumatology_biologics_data()
                client.save_data_to_hf(data, HF_TOKEN, DATASET_NAME)
                st.success("Data updated successfully")
                st.rerun()
            except Exception as e:
                st.error(f"An error occurred while updating data: {str(e)}")

def main():
    st.set_page_config(
        page_title="Biologics Prescriber Helper",
        page_icon="💊",
        layout="wide"
    )
    
    st.title("Biologics Prescriber Helper")
    
    # Load data
    if 'data' not in st.session_state:
        st.session_state.data = load_data()
    
    # Initialize all session state variables
    fields = ['drug', 'brand', 'formulation', 'indication', 'treatment_phase', 'hospital_type']
    
    # Initialize selections and reset flag
    if 'selections' not in st.session_state:
        st.session_state.selections = {field: '' for field in fields}
    
    if 'reset' not in st.session_state:
        st.session_state.reset = False
    
    # Create callback for selection changes
    def update_selection(field):
        st.session_state.selections[field] = st.session_state[f"select_{field}"]
    
    # Handle reset
    if st.session_state.reset:
        st.session_state.selections = {field: '' for field in fields}
        st.session_state.reset = False
    
    # Get valid options based on current selections
    options = get_valid_options(
        st.session_state.data['combinations'], 
        {k: v if v != '' else None for k, v in st.session_state.selections.items()}
    )
    
    # Create columns for the dropdowns
    col1, col2 = st.columns(2)
    
    # Create dropdowns
    with col1:
        st.selectbox(
            "Drug",
            options=[''] + options['drugs'],
            key="select_drug",
            index=0 if not st.session_state.selections['drug'] else 
                  ([''] + options['drugs']).index(st.session_state.selections['drug']),
            on_change=update_selection,
            args=('drug',)
        )
        
        st.selectbox(
            "Brand",
            options=[''] + options['brands'],
            key="select_brand",
            index=0 if not st.session_state.selections['brand'] else 
                  ([''] + options['brands']).index(st.session_state.selections['brand']),
            on_change=update_selection,
            args=('brand',)
        )
        
        st.selectbox(
            "Formulation",
            options=[''] + options['formulations'],
            key="select_formulation",
            index=0 if not st.session_state.selections['formulation'] else 
                  ([''] + options['formulations']).index(st.session_state.selections['formulation']),
            on_change=update_selection,
            args=('formulation',)
        )
    
    with col2:
        st.selectbox(
            "Indication",
            options=[''] + options['indications'],
            key="select_indication",
            index=0 if not st.session_state.selections['indication'] else 
                  ([''] + options['indications']).index(st.session_state.selections['indication']),
            on_change=update_selection,
            args=('indication',)
        )
        
        st.selectbox(
            "Treatment Phase",
            options=[''] + options['treatment_phases'],
            key="select_treatment_phase",
            index=0 if not st.session_state.selections['treatment_phase'] else 
                  ([''] + options['treatment_phases']).index(st.session_state.selections['treatment_phase']),
            on_change=update_selection,
            args=('treatment_phase',)
        )
        
        st.selectbox(
            "Hospital Type",
            options=[''] + options['hospital_types'],
            key="select_hospital_type",
            index=0 if not st.session_state.selections['hospital_type'] else 
                  ([''] + options['hospital_types']).index(st.session_state.selections['hospital_type']),
            on_change=update_selection,
            args=('hospital_type',)
        )
    
    # Create buttons
    col1, col2 = st.columns(2)
    with col1:
        if st.button("Search", type="primary"):
            search_biologics(
                st.session_state.data['combinations'],
                {k: v if v != '' else None for k, v in st.session_state.selections.items()}
            )
    
    with col2:
        if st.button("Reset"):
            st.session_state.reset = True
            st.rerun()

if __name__ == "__main__":
    if UPDATE_INTERVAL > 0:
        # Set up the scheduler
        update_data()
        scheduler = BackgroundScheduler()
        scheduler.add_job(
            func=update_data,
            trigger=IntervalTrigger(days=UPDATE_INTERVAL),
            id='update_data',
            name='Update Data',
            replace_existing=True
        )
        scheduler.start()
        atexit.register(lambda: scheduler.shutdown())
    
    main()