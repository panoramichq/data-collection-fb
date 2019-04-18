# flake8: noqa: E722
"""
Module contains switches governing what jobs are to be *disabled*
from providing expectations. The job code itself is not disabled,
but the system will skip trying to create expectations for these
job variants.

Note on implementation:
While another alternative to this is explicitly mark what jobs are
*enabled*, if you want to have group management of the jobs,
it's hard to interpret / utilize in logic instruction:
    ALL METRICS ENABLED = True
    SOME METRICS JOB ENABLED = False
As you don't know what the priority is.

With "disabled" interpreting group DISABLED=True
is more meaningful in context of some particular job DISABLED=False
In this case, group level DISABLED clearly takes precedence.
"""

# Activate Job Gatekeeper
ACTIVATE_JOB_GATEKEEPER = True
REPORT_TYPE_LIFETIME_FREQUENCY = 6
REPORT_TYPE_LIFETIME_PAGE_POSTS_FREQUENCY = 10
REPORT_TYPE_LIFETIME_PAGE_VIDEOS_FREQUENCY = 9
REPORT_TYPE_ENTITY_FREQUENCY = 2
REPORT_TYPE_ENTITY_COMMENTS_FREQUENCY = 4

# Ad account sourcing
AD_ACCOUNT_IMPORT_DISABLED = False
PAGE_IMPORT_DISABLED = False

# actual entity jobs:
ENTITY_AA_DISABLED = False
ENTITY_C_DISABLED = False
ENTITY_AS_DISABLED = False
ENTITY_A_DISABLED = False
ENTITY_AC_DISABLED = False
ENTITY_AV_DISABLED = False
ENTITY_CA_DISABLED = False
ENTITY_P_DISABLED = False
ENTITY_PP_DISABLED = False
ENTITY_PV_DISABLED = False
ENTITY_CM_DISABLED = False

# group flag for entities
ENTITY_ALL_DISABLED = False

# actual insights jobs:
INSIGHTS_LIFETIME_C_DISABLED = False
INSIGHTS_LIFETIME_AS_DISABLED = False
INSIGHTS_LIFETIME_A_DISABLED = False
INSIGHTS_LIFETIME_PV_DISABLED = False
INSIGHTS_LIFETIME_PP_DISABLED = False
INSIGHTS_LIFETIME_P_DISABLED = False
# Not tested / used yet
INSIGHTS_HOUR_C_DISABLED = True  # TODO: Switch to False once we verify pipeline needs it
INSIGHTS_HOUR_AS_DISABLED = True  # TODO: Switch to False once we verify pipeline needs it

# Definitely used
INSIGHTS_AGE_GENDER_A_DISABLED = False
INSIGHTS_DAY_A_DISABLED = False
INSIGHTS_DMA_A_DISABLED = False
INSIGHTS_HOUR_A_DISABLED = False
INSIGHTS_PLATFORM_A_DISABLED = False

# group flags for insights jobs
INSIGHTS_ALL_DISABLED = False
INSIGHTS_ALL_ORGANIC_DISABLED = True
INSIGHTS_ALL_LIFETIME_DISABLED = False
INSIGHTS_ALL_SEGMENTED_DISABLED = False

# here we allow external env vars to influence above values...
from common.updatefromenv import update_from_env

update_from_env(__name__)

# and here we post-process the values, by applying group
if ENTITY_ALL_DISABLED:
    ENTITY_C_DISABLED = ENTITY_AS_DISABLED = ENTITY_A_DISABLED = ENTITY_AC_DISABLED = ENTITY_AV_DISABLED = True
    ENTITY_CA_DISABLED = ENTITY_AA_DISABLED = ENTITY_P_DISABLED = ENTITY_PP_DISABLED = ENTITY_CM_DISABLED = True
    ENTITY_PV_DISABLED = True

if INSIGHTS_ALL_DISABLED:
    INSIGHTS_ALL_LIFETIME_DISABLED = INSIGHTS_ALL_SEGMENTED_DISABLED = True

if INSIGHTS_ALL_LIFETIME_DISABLED:
    INSIGHTS_LIFETIME_C_DISABLED = True
    INSIGHTS_LIFETIME_AS_DISABLED = True
    INSIGHTS_LIFETIME_A_DISABLED = True
    INSIGHTS_LIFETIME_PV_DISABLED = True
    INSIGHTS_LIFETIME_PP_DISABLED = True
    INSIGHTS_LIFETIME_P_DISABLED = True

if INSIGHTS_ALL_SEGMENTED_DISABLED:
    INSIGHTS_AGE_GENDER_A_DISABLED = True
    INSIGHTS_DAY_A_DISABLED = True
    INSIGHTS_DMA_A_DISABLED = True
    INSIGHTS_HOUR_A_DISABLED = True
    INSIGHTS_HOUR_AS_DISABLED = True
    INSIGHTS_HOUR_C_DISABLED = True
    INSIGHTS_PLATFORM_A_DISABLED = True

if INSIGHTS_ALL_ORGANIC_DISABLED:
    INSIGHTS_LIFETIME_P_DISABLED = True
    INSIGHTS_LIFETIME_PP_DISABLED = True
    INSIGHTS_LIFETIME_PV_DISABLED = True
