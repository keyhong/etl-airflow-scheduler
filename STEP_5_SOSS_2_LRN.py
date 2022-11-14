#!/usr/bin/env python3
# coding: utf-8

######################################################
#    프로그램명    : STEP_5_SOSS_2_LRN.py
#    작성자        : Gyu Won Hong
#    작성일자      : 2022.11.01
#    파라미터      : 
#    설명          : 모델학습 하는 TASK들의 매월 1일 15시에 실행하는 Airflow 스케줄러 모듈
#    변경일자      :
#    변경내역      :
######################################################

from airflow.operators.bash_operator import BashOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.models import DAG
from datetime import datetime, timedelta
import pendulum
# 로컬 타임존 생성
local_tz = pendulum.timezone("Asia/Seoul")

# Set DAG args
args = {
    'owner': 'GyuWon Hong',
    'start_date': datetime(2022, 11, 1, tzinfo=local_tz),
    'retries': 3,
    'retry_delay': timedelta(minutes=5)    
}

with DAG(  # With문으로 인스턴스를 생성하여 ":" 아래 구절이 다 실행된후에는 객체 자동 제거 실행
    dag_id='STEP_5_SOSS_2_LRN',
    description= 'SOSS SAFE_IDEX PPRCS·LRN, CCTV_OPTMZ LRN DAG',
    default_args=args,
    schedule_interval="0 15 1 * *"   # 월 배치 스케줄링 (매월 1일 15:00) 
) as dag:

    # START (Dummy)
    STEP_5_SOSS_2_LRN_START = DummyOperator(
        task_id='STEP_5_SOSS_2_LRN_START.task1',
        dag=dag,
    )

    # 안전지수 전처리
    SOSS_SAFE_IDEX_PPRCS = BashOperator(
        task_id='SOSS_SAFE_IDEX_PPRCS.task1',
        bash_command='/lake_etl/etl/mart/soss/c_SOSS_SAFE_IDEX_PPRCS_D_01.sh {{ next_ds_nodash }} 000000 SOSS',
        dag=dag,
    )

    # 안전지수 모델 학습
    SOSS_SAFE_IDEX_LRN = BashOperator(
        task_id='SOSS_SAFE_IDEX_LRN.task1',
        bash_command='/lake_etl/etl/mart/soss/c_SOSS_SAFE_IDEX_LRN_D_01.sh {{ next_ds_nodash }} 000000 SOSS',
        dag=dag,
    )

    # CCTV 최적화 모델 학습
    SOSS_CCTV_OPTMZ_LRN = BashOperator(
        task_id='SOSS_CCTV_OPTMZ_LRN.task1',
        bash_command='/lake_etl/etl/mart/soss/c_SOSS_CCTV_OPTMZ_LRN_D_01.sh {{ next_ds_nodash }} 000000 SOSS',
        dag=dag,
    )
    
    # END (Dummy)
    STEP_5_SOSS_2_LRN_END = DummyOperator(
        task_id='STEP_5_SOSS_2_LRN_END.task1',
        dag=dag,
    )

    STEP_5_SOSS_2_LRN_START >> SOSS_SAFE_IDEX_PPRCS >> SOSS_SAFE_IDEX_LRN >> SOSS_CCTV_OPTMZ_LRN >> STEP_5_SOSS_2_LRN_END

    # 만약 9시 이전의 task를 실행날짜와 동일하게 돌리고 싶다면 [ macro.datetime, macro.datetutil ] 을 이용해 jinja2 템플릿의 시간을 조정할 수 있다. (python의 datetime, dateutil과 동일)
    # 9시 이전에 작동되는 코드인 경우 사용 : macros.datetime.strftime(macros.datetime.strptime(ts_nodash, '%Y%m%dT%H%M%S') + macros.dateutil.relativedelta.relativedelta(months=1, hours=9), '%Y%m%d')