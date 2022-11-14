#!/usr/bin/env python3
# coding: utf-8

######################################################
#    프로그램명    : STEP_5_SOSS_1_OPER.py
#    작성자        : Gyu Won Hong
#    작성일자      : 2022.11.01
#    파라미터      : 
#    설명          : 모델 운영하는 TASK들이 매일 이전 DAG가 끝나면 트리거 되어 실행하는 Airflow 스케줄러 모듈
#    변경일자      :
#    변경내역      :
######################################################

from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.bash_operator import BashOperator
from airflow.models import DAG
from datetime import datetime, timedelta

import pendulum
## 로컬 타임존 생성
local_tz = pendulum.timezone("Asia/Seoul")

args = {
    'owner': 'GyuWon Hong',
    'start_date': datetime(2021, 9, 16, tzinfo=local_tz),
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
    'schedule_interval': None
}

# With문으로 인스턴스를 생성하여 ":" 아래 구절이 다 실행된후에는 객체 자동 제거 실행
with DAG(  
    dag_id='STEP_5_SOSS_1_OPER',
    description= 'SAFE_IDEX OPER, CCTV_OPTMZ OPER, PTR_PST_RCM_OPER을 순차적으로 실행',
    default_args=args,
) as dag:
    dag.doc_md = __doc__

    # START (Dummy)
    STEP_5_SOSS_1_OPER_START = DummyOperator(task_id='STEP_5_SOSS_1_OPER_START.task1', dag=dag)

    # 안전지수 
    SOSS_SAFE_IDEX_OPER  = BashOperator(
        task_id='SOSS_SAFE_IDEX_OPER.task1',
        bash_command='/lake_etl/etl/mart/soss/c_SOSS_SAFE_IDEX_OPER_D_01.sh {{ tomorrow_ds_nodash }} 000000 SOSS',
        dag=dag,
    )
    SOSS_SAFE_IDEX_OPER.doc_md = '안전지수 분석 운영 모듈'

    # CCTV 효율지수 
    SOSS_CCTV_OPTMZ_OPER = BashOperator(
        task_id='SOSS_CCTV_OPTMZ_OPER.task1',
        bash_command='/lake_etl/etl/mart/soss/c_SOSS_CCTV_OPTMZ_OPER_D_01.sh {{ tomorrow_ds_nodash }} 000000 SOSS',
        dag=dag,
    )
    SOSS_CCTV_OPTMZ_OPER.doc_md = 'CCTV 효율지수 분석 운영 모듈'

    # 순찰거점추천
    SOSS_PTR_PST_RCM_OPER = BashOperator(
        task_id='SOSS_PTR_PST_RCM_OPER.task1',
        bash_command='/lake_etl/etl/mart/soss/c_SOSS_PTR_PST_RCM_OPER_D_01.sh {{ tomorrow_ds_nodash }} 000000 SOSS',
        dag=dag,
    )
    SOSS_PTR_PST_RCM_OPER.doc_md = '순찰거점 분석 운영 모듈'

    # 경찰청 신고접수 통계 
    DM_NPA_DCR_RCP_STT  = BashOperator(
        task_id='DM_NPA_DCR_RCP_STT.task1',
        bash_command='/lake_etl/etl/mart/soss/c_DM_NPA_DCR_RCP_STT_D_01.sh {{ tomorrow_ds_nodash }} 000000 SOSS',
        dag=dag,
    )
    DM_NPA_DCR_RCP_STT.doc_md = '경찰청 신고접수 통계 쉘 스크립트'

    # 구별 CCTV 모니터 비율
    DM_GU_CCTV_MNTR_RATE  = BashOperator(
        task_id='DM_GU_CCTV_MNTR_RATE.task1',
        bash_command='/lake_etl/etl/mart/soss/c_DM_GU_CCTV_MNTR_RATE_D_01.sh {{ tomorrow_ds_nodash }} 000000 SOSS',
        dag=dag,
    )
    DM_GU_CCTV_MNTR_RATE.doc_md = '구별 CCTV 모니터 비율 통계 쉘 스크립트'

    # 구별 CCTV 모니터 시간
    DM_GU_CCTV_MNTR_TM  = BashOperator(
        task_id='DM_GU_CCTV_MNTR_TM.task1',
        bash_command='/lake_etl/etl/mart/soss/c_DM_GU_CCTV_MNTR_TM_D_01.sh {{ tomorrow_ds_nodash }} 000000 SOSS',
        dag=dag,
    )
    DM_GU_CCTV_MNTR_RATE.doc_md = '구별 CCTV 모니터 시간 통계 쉘 스크립트'

    # END (Dummy)
    STEP_5_SOSS_1_OPER_END = DummyOperator(task_id='STEP_5_SOSS_1_OPER_END.task1', dag=dag)
    
    # GROUP1 (Dummy)
    STEP_5_SOSS_1_OPER_GR1 = DummyOperator(
        task_id='STEP_5_SOSS_1_OPER_GR1.task1',
        dag=dag,
    )

    # GROUP2 (Dummy)
    STEP_5_SOSS_1_OPER_GR2 = DummyOperator(
        task_id='STEP_5_SOSS_1_OPER_GR2.task1',
        dag=dag,
    )

    # GROUP3 (Dummy)
    STEP_5_SOSS_1_OPER_GR3 = DummyOperator(
        task_id='STEP_5_SOSS_1_OPER_GR3.task1',
        dag=dag,
    )

    STEP_5_SOSS_1_OPER_START >> STEP_5_SOSS_1_OPER_GR1
    STEP_5_SOSS_1_OPER_GR1   >> SOSS_SAFE_IDEX_OPER                                              >> STEP_5_SOSS_1_OPER_GR2
    STEP_5_SOSS_1_OPER_GR2   >> [ SOSS_CCTV_OPTMZ_OPER, SOSS_PTR_PST_RCM_OPER ]                  >> STEP_5_SOSS_1_OPER_GR3
    STEP_5_SOSS_1_OPER_GR3   >> [ DM_NPA_DCR_RCP_STT, DM_GU_CCTV_MNTR_RATE, DM_GU_CCTV_MNTR_TM ] >> STEP_5_SOSS_1_OPER_END 

