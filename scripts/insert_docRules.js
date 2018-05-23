db.getCollection('DocCardExtractorRule').insert([
    {
        "docType": "PEOPLE",
        "createTime": ISODate("2017-12-15T09:59:49.106Z"),
        "lastModifyTime": ISODate("2017-12-16T09:59:49.106Z"),
        "elpModel": "standard_model",
        "elpType": "permanent_population",
        "key": "person_ssn_id",
        "docCollectionName": "doc_card_index_info",
        "propertyMappings": [
            {
                "needRelation": false,
                "sPropertyName": "person_name",
                "dPropertyName": "tab_s_name",
                "sourceTargetEntities": []
            },
            {
                "needRelation": false,
                "sPropertyName": "person_residential_address",
                "dPropertyName": "tab_s_residentialAddress",
                "sourceTargetEntities": []
            },
            {
                "needRelation": false,
                "sPropertyName": "person_ssn_id",
                "dPropertyName": "tab_s_sid",
                "sourceTargetEntities": []
            },
            {
                "needRelation": true,
                "sPropertyName": "",
                "dPropertyName": "tab_m_tag",
                "sourceTargetEntities": [
                    {
                        "linkedRelationType": "",
                        "onlyFromRel": false,
                        "targetEntityType": "special_persion_record",
                        "description": "人员类型",
                        "sPropertyName": "spr_special_type",
                        "sPropertyId": "person_ssn_id"
                    }
                ]
            },
            {
                "needRelation": true,
                "description": "人员标签",
                "sPropertyName": "",
                "dPropertyName": "tab_s_showTag",
                "sourceTargetEntities": [
                    {
                        "linkedRelationType": "",
                        "onlyFromRel": false,
                        "targetEntityType": "special_persion_record",
                        "description": "人员标签",
                        "sPropertyName": "person_ssn_id",
                        "sPropertyId": "person_ssn_id"
                    }
                ]
            },
            {
                "needRelation": false,
                "description": "婚姻状况",
                "sPropertyName": "person_marital_status",
                "dPropertyName": "tab_s_person_marital_status",
                "sourceTargetEntities": []
            },
            {
                "needRelation": false,
                "description": "籍贯地址",
                "sPropertyName": "person_origin_area",
                "dPropertyName": "tab_s_person_origin_area",
                "sourceTargetEntities": []
            },
            {
                "needRelation": true,
                "description": "就业单位标识",
                "sPropertyName": "eb_company_id",
                "dPropertyName": "tab_m_eb_company_id",
                "sourceTargetEntities": [
                    {
                        "linkedRelationType": "employed_by",
                        "onlyFromRel": true,
                        "targetEntityType": "",
                        "description": "就业单位标识",
                        "sPropertyName": "eb_company_id",
                        "sPropertyId": "eb_person_id"
                    }
                ]
            },
            {
                "needRelation": true,
                "description": "就业单位名称",
                "sPropertyName": "eb_company_name",
                "dPropertyName": "tab_m_eb_company_name",
                "sourceTargetEntities": [
                    {
                        "linkedRelationType": "employed_by",
                        "onlyFromRel": true,
                        "targetEntityType": "",
                        "description": "就业单位名称",
                        "sPropertyName": "eb_company_name",
                        "sPropertyId": "eb_person_id"
                    }
                ]
            },
            {
                "needRelation": false,
                "description": "性别",
                "sPropertyName": "person_gender",
                "dPropertyName": "persone_sex",
                "sourceTargetEntities": []
            },
            {
                "needRelation": false,
                "description": "居住地址",
                "sPropertyName": "person_residential_address",
                "dPropertyName": "persone_reg",
                "sourceTargetEntities": []
            }
        ],
        "indexPropertyMappings": [
            {
                "needRelation": true,
                "description": "就业单位名称",
                "sPropertyName": "eb_company_name",
                "dPropertyName": "d_organization_name",
                "sourceTargetEntities": [
                    {
                        "linkedRelationType": "employed_by",
                        "onlyFromRel": true,
                        "targetEntityType": "",
                        "description": "就业单位名称",
                        "sPropertyName": "eb_company_name",
                        "sPropertyId": "eb_person_id"
                    }
                ]
            },
            {
                "needRelation": false,
                "sPropertyName": "person_name",
                "dPropertyName": "d_name_name",
                "sourceTargetEntities": []
            },
            {
                "needRelation": false,
                "sPropertyName": "person_ssn_id",
                "dPropertyName": "d_card_sid",
                "sourceTargetEntities": []
            },
            {
                "needExtendsion": true,
                "sourceTargetEntities": [
                    {
                        "linkedRelationType": "person_mobile",
                        "onlyFromRel": true,
                        "targetEntityType": "",
                        "description": "手机号",
                        "level": "first",
                        "sPropertyName": "mobile_number",
                        "dPropertyName": "d_phone_number",
                        "sPropertyId": "person_ssn_id",
                        "secondLevel": [
                            {
                                "linkedRelationType": "",
                                "onlyFromRel": false,
                                "targetEntityType": "qq_account",
                                "description": "QQ号，从实体上取",
                                "level": "second",
                                "sPropertyName": "qq_account",
                                "dPropertyName": "d_qq_number",
                                "sPropertyId": "qq_mobile_number"
                            },
                            {
                                "linkedRelationType": "",
                                "onlyFromRel": false,
                                "targetEntityType": "wechat_account",
                                "description": "微信号，从实体上取",
                                "level": "second",
                                "sPropertyName": "wechat_account",
                                "dPropertyName": "d_wchart_webchart",
                                "sPropertyId": "wb_mobile_number"
                            }
                        ]
                    }
                ]
            },
            {
                "needRelation": true,
                "sPropertyName": "",
                "dPropertyName": "d_auto_number",
                "sourceTargetEntities": [
                    {
                        "linkedRelationType": "",
                        "onlyFromRel": false,
                        "targetEntityType": "vehicle",
                        "description": "车牌号，车辆实体取",
                        "sPropertyName": "vehicle_plate_code",
                        "sPropertyId": "vehicle_owner_ssn_id"
                    }
                ]
            },
            {
                "needRelation": false,
                "sPropertyName": "person_residential_address",
                "dPropertyName": "d_addr_addr",
                "sourceTargetEntities": []
            },
            {
                "needRelation": false,
                "sPropertyName": "person_birth_area",
                "dPropertyName": "d_addr_addr",
                "sourceTargetEntities": []
            },
            {
                "needRelation": false,
                "sPropertyName": "person_birth_address",
                "dPropertyName": "d_addr_addr",
                "sourceTargetEntities": []
            },
            {
                "needRelation": false,
                "sPropertyName": "person_residential_area",
                "dPropertyName": "d_addr_addr",
                "sourceTargetEntities": []
            },
            {
                "needRelation": false,
                "sPropertyName": "person_residential_district",
                "dPropertyName": "d_addr_addr",
                "sourceTargetEntities": []
            },
            {
                "needRelation": true,
                "sPropertyName": "",
                "dPropertyName": "d_case_number",
                "sourceTargetEntities": [
                    {
                        "linkedRelationType": "people_involved_case",
                        "onlyFromRel": true,
                        "targetEntityType": "",
                        "description": "案件号,从链接上取,人员涉案",
                        "sPropertyName": "pic_case_no",
                        "sPropertyId": "pic_person_ssn_id"
                    }
                ]
            }
        ]
    },
    {
        "docType": "VEHICLE",
        "createTime": ISODate("2017-12-15T17:59:49.106+08:00"),
        "lastModifyTime": ISODate("2017-12-16T17:59:49.106+08:00"),
        "elpModel": "standard_model",
        "elpType": "vehicle",
        "key": "vehicle_plate_code",
        "docCollectionName": "doc_card_index_info",
        "propertyMappings": [
            {
                "needRelation": false,
                "sPropertyName": "vehicle_plate_code",
                "dPropertyName": "tab_s_vehicle_plate_code",
                "description": "车牌号",
                "sourceTargetEntities": []
            },
            {
                "needRelation": false,
                "sPropertyName": "vehicle_brand",
                "dPropertyName": "tab_s_vehicle_brand",
                "description": "品牌",
                "sourceTargetEntities": []
            },
            {
                "needRelation": false,
                "sPropertyName": "vehicle_brand",
                "dPropertyName": "arti_auto_brand",
                "description": "品牌",
                "sourceTargetEntities": []
            },
            {
                "needRelation": false,
                "sPropertyName": "vehicle_first_register_date",
                "dPropertyName": "tab_s_registrationDate",
                "description": "登记时间",
                "sourceTargetEntities": []
            },
            {
                "needRelation": false,
                "description": "车主身份证号",
                "sPropertyName": "vehicle_owner_ssn_id",
                "dPropertyName": "tab_s_owner_ssn_id",
                "sourceTargetEntities": []
            },
            {
                "needRelation": false,
                "description": "车主姓名",
                "sPropertyName": "vehicle_owner_name",
                "dPropertyName": "tab_s_owner_name",
                "sourceTargetEntities": []
            },
            {
                "needRelation": false,
                "sPropertyName": "owner_address",
                "dPropertyName": "tab_s_residentialAddress",
                "description": "车主现居地址",
                "sourceTargetEntities": []
            },
            {
                "needRelation": false,
                "sPropertyName": "owner_telphone",
                "dPropertyName": "tab_s_phoneNumber",
                "description": "所有人手机号",
                "sourceTargetEntities": []
            },
            {
                "needRelation": false,
                "sPropertyName": "vehicle_id_number",
                "dPropertyName": "tab_s_vehicle_id_number",
                "description": "车辆识别号",
                "sourceTargetEntities": []
            },
            {
                "needRelation": false,
                "sPropertyName": "vehicle_model",
                "dPropertyName": "tab_s_vehicle_model",
                "description": "型号",
                "sourceTargetEntities": []
            },
            {
                "needRelation": false,
                "sPropertyName": "vehicle_vehicle_type",
                "dPropertyName": "tab_s_vehicleType",
                "description": "车辆种类",
                "sourceTargetEntities": []
            },
            {
                "needRelation": false,
                "sPropertyName": "vehicle_displacement",
                "dPropertyName": "tab_s_output",
                "description": "排量",
                "sourceTargetEntities": []
            }
        ],
        "indexPropertyMappings": [
            {
                "needRelation": false,
                "sPropertyName": "vehicle_owner_name",
                "dPropertyName": "d_name_name",
                "sourceTargetEntities": []
            },
            {
                "needRelation": false,
                "sPropertyName": "vehicle_owner_ssn_id",
                "dPropertyName": "d_card_sid",
                "sourceTargetEntities": []
            },
            {
                "needRelation": false,
                "description": "手机号",
                "sPropertyName": "owner_telphone",
                "dPropertyName": "d_phone_number",
                "sourceTargetEntities": []
            },
            {
                "needRelation": false,
                "description": "车牌号",
                "sPropertyName": "vehicle_plate_code",
                "dPropertyName": "d_auto_number",
                "sourceTargetEntities": []
            },
            {
                "needRelation": false,
                "description": "地址",
                "sPropertyName": "owner_address",
                "dPropertyName": "d_addr_addr",
                "sourceTargetEntities": []
            }
        ]
    },
    {
        "docType": "TELPHONE",
        "createTime": ISODate("2017-12-15T17:59:49.106+08:00"),
        "lastModifyTime": ISODate("2017-12-16T17:59:49.106+08:00"),
        "elpModel": "standard_model",
        "elpType": "phone_number",
        "key": "mobile_number",
        "docCollectionName": "doc_card_index_info",
        "propertyMappings": [
            {
                "needRelation": false,
                "sPropertyName": "mobile_number",
                "dPropertyName": "tab_s_phoneNumber",
                "sourceTargetEntities": []
            },
            {
                "needRelation": false,
                "sPropertyName": "mobile_home_area",
                "dPropertyName": "tab_s_home_area",
                "sourceTargetEntities": []
            },
            {
                "needRelation": false,
                "sPropertyName": "mobile_owner_name",
                "dPropertyName": "tab_s_owner_name",
                "sourceTargetEntities": []
            },
            {
                "needRelation": true,
                "sPropertyName": "",
                "dPropertyName": "tab_s_residentialAddress",
                "sourceTargetEntities": [
                    {
                        "linkedRelationType": "",
                        "onlyFromRel": false,
                        "targetEntityType": "permanent_population",
                        "description": "机主现居地址,从常住人口拿",
                        "sPropertyName": "person_residential_address",
                        "sPropertyId": "user_telphone"
                    }
                ]
            },
            {
                "needRelation": false,
                "sPropertyName": "mobile_imisi",
                "dPropertyName": "tab_s_mobile_imisi",
                "sourceTargetEntities": []
            },
            {
                "needRelation": false,
                "sPropertyName": "mobile_imei",
                "dPropertyName": "tab_s_mobile_imei",
                "sourceTargetEntities": []
            },
            {
                "needRelation": false,
                "sPropertyName": "register_time",
                "dPropertyName": "tab_s_registerDate",
                "sourceTargetEntities": []
            }
        ],
        "indexPropertyMappings": [
            {
                "needRelation": false,
                "sPropertyName": "mobile_owner_name",
                "dPropertyName": "d_name_name",
                "sourceTargetEntities": []
            },
            {
                "needRelation": true,
                "sPropertyName": "person_ssn_id",
                "dPropertyName": "d_card_sid",
                "sourceTargetEntities": [
                    {
                        "linkedRelationType": "person_mobile",
                        "onlyFromRel": true,
                        "targetEntityType": "",
                        "description": "",
                        "sPropertyName": "person_ssn_id",
                        "sPropertyId": "mobile_number"
                    }
                ]
            },
            {
                "needRelation": false,
                "sPropertyName": "mobile_number",
                "dPropertyName": "d_phone_number",
                "sourceTargetEntities": []
            },
            {
                "needRelation": true,
                "sPropertyName": "",
                "dPropertyName": "d_wchart_webchart",
                "sourceTargetEntities": [
                    {
                        "linkedRelationType": "",
                        "onlyFromRel": false,
                        "targetEntityType": "wechat_account",
                        "description": "微信号",
                        "sPropertyName": "wechat_account",
                        "sPropertyId": "wb_mobile_number"
                    }
                ]
            },
            {
                "needRelation": true,
                "sPropertyName": "",
                "dPropertyName": "d_qq_number",
                "sourceTargetEntities": [
                    {
                        "linkedRelationType": "",
                        "onlyFromRel": false,
                        "targetEntityType": "qq_account",
                        "description": "QQ号",
                        "sPropertyName": "qq_account",
                        "sPropertyId": "qq_mobile_number"
                    }
                ]
            },
            {
                "needRelation": true,
                "sPropertyName": "",
                "dPropertyName": "d_auto_number",
                "sourceTargetEntities": [
                    {
                        "linkedRelationType": "",
                        "onlyFromRel": false,
                        "targetEntityType": "vehicle",
                        "description": "车牌号",
                        "sPropertyName": "vehicle_plate_code",
                        "sPropertyId": "owner_telphone"
                    }
                ]
            },
            {
                "needRelation": false,
                "sPropertyName": "mobile_home_area",
                "dPropertyName": "d_addr_addr",
                "sourceTargetEntities": []
            },
            {
                "needRelation": true,
                "sPropertyName": "",
                "dPropertyName": "d_case_number",
                "sourceTargetEntities": [
                    {
                        "linkedRelationType": "",
                        "onlyFromRel": false,
                        "targetEntityType": "case",
                        "description": "案件号, 办案人员手机号",
                        "sPropertyName": "case_no",
                        "sPropertyId": "case_operator_tel"
                    }
                ]
            },
            {
                "needRelation": true,
                "sPropertyName": "",
                "dPropertyName": "d_case_number",
                "sourceTargetEntities": [
                    {
                        "linkedRelationType": "",
                        "onlyFromRel": false,
                        "targetEntityType": "suspect",
                        "description": "案件号,嫌疑人手机号",
                        "sPropertyName": "case_no",
                        "sPropertyId": "suspect_telphone"
                    }
                ]
            }
        ]
    },
    {
        "docType": "ORGANIZATION",
        "createTime": ISODate("2017-12-15T17:59:49.106+08:00"),
        "lastModifyTime": ISODate("2017-12-16T17:59:49.106+08:00"),
        "elpModel": "standard_model",
        "elpType": "company",
        "key": "company_no",
        "docCollectionName": "doc_card_index_info",
        "propertyMappings": [
            {
                "needRelation": false,
                "description": "企业编号",
                "sPropertyName": "company_no",
                "dPropertyName": "tab_s_company_id",
                "sourceTargetEntities": []
            },
            {
                "needRelation": false,
                "description": "公司名称",
                "sPropertyName": "company_name",
                "dPropertyName": "tab_s_companyName",
                "sourceTargetEntities": []
            },
            {
                "needRelation": false,
                "description": "机构注册时间",
                "sPropertyName": "company_register_date",
                "dPropertyName": "tab_s_registerDate",
                "sourceTargetEntities": []
            },
            {
                "needRelation": false,
                "description": "机构地址",
                "sPropertyName": "company_address",
                "dPropertyName": "tab_s_address",
                "sourceTargetEntities": []
            },
            {
                "needRelation": false,
                "description": "组织机构代码",
                "sPropertyName": "company_social_credit_code",
                "dPropertyName": "tab_s_company_social_credit_code",
                "sourceTargetEntities": []
            },
            {
                "needRelation": false,
                "description": "法人身份证",
                "sPropertyName": "company_legal_persion_id",
                "dPropertyName": "tab_s_company_legal_persion_id",
                "sourceTargetEntities": []
            },
            {
                "needRelation": false,
                "description": "法人姓名",
                "sPropertyName": "company_legal_persion_name",
                "dPropertyName": "tab_s_personName",
                "sourceTargetEntities": []
            },
            {
                "needRelation": false,
                "description": "地址所属行政区域（粒度到区级）",
                "sPropertyName": "company_address",
                "dPropertyName": "tab_s_company_address",
                "sourceTargetEntities": []
            },
            {
                "needRelation": false,
                "description": "二级搜索所属区域",
                "sPropertyName": "company_address",
                "dPropertyName": "org_com_region",
                "sourceTargetEntities": []
            }
        ],
        "indexPropertyMappings": [
            {
                "needRelation": false,
                "description": "公司名称",
                "sPropertyName": "company_name",
                "dPropertyName": "d_organization_name",
                "sourceTargetEntities": []
            },
            {
                "needRelation": false,
                "description": "姓名",
                "sPropertyName": "company_legal_persion_name",
                "dPropertyName": "d_name_name",
                "sourceTargetEntities": []
            },
            {
                "needRelation": false,
                "description": "身份证号码",
                "sPropertyName": "company_legal_persion_id",
                "dPropertyName": "d_card_sid",
                "sourceTargetEntities": []
            },
            {
                "needRelation": true,
                "sPropertyName": "",
                "dPropertyName": "d_auto_number",
                "sourceTargetEntities": [
                    {
                        "linkedRelationType": "organization_has_vehicle",
                        "onlyFromRel": true,
                        "targetEntityType": "",
                        "description": "车牌号",
                        "sPropertyName": "vehicle_number",
                        "sPropertyId": "organization_no"
                    }
                ]
            },
            {
                "needRelation": false,
                "sPropertyName": "company_address",
                "dPropertyName": "d_addr_addr",
                "sourceTargetEntities": []
            }
        ]
    },
    {
        "docType": "CASE",
        "createTime": ISODate("2017-12-15T17:59:49.106+08:00"),
        "lastModifyTime": ISODate("2017-12-16T17:59:49.106+08:00"),
        "elpModel": "standard_model",
        "elpType": "case",
        "key": "case_no",
        "docCollectionName": "doc_card_index_info",
        "propertyMappings": [
            {
                "needRelation": false,
                "description": "案件标识",
                "sPropertyName": "case_no",
                "dPropertyName": "tab_s_caseId",
                "sourceTargetEntities": []
            },
            {
                "needRelation": false,
                "description": "案件名称",
                "sPropertyName": "case_name",
                "dPropertyName": "tab_s_caseName",
                "sourceTargetEntities": []
            },
            {
                "needRelation": false,
                "description": "案件状态",
                "sPropertyName": "case_status",
                "dPropertyName": "tab_s_caseStatus",
                "sourceTargetEntities": []
            },
            {
                "needRelation": false,
                "description": "案发时间，取发案时间初值",
                "sPropertyName": "case_begin_time",
                "dPropertyName": "tab_s_findDate",
                "sourceTargetEntities": []
            },
            {
                "needRelation": false,
                "description": "案件类型",
                "sPropertyName": "case_category",
                "dPropertyName": "tab_s_caseType",
                "sourceTargetEntities": []
            },
            {
                "needRelation": false,
                "description": "二级搜索案件类型",
                "sPropertyName": "case_category",
                "dPropertyName": "case_type",
                "sourceTargetEntities": []
            },
            {
                "needRelation": false,
                "description": "二级搜索地址",
                "sPropertyName": "case_area",
                "dPropertyName": "addr_region",
                "sourceTargetEntities": []
            },
            {
                "needRelation": false,
                "sPropertyName": "case_area",
                "dPropertyName": "tab_s_belongArea",
                "sourceTargetEntities": []
            },
            {
                "needRelation": true,
                "description": "嫌疑人姓名",
                "sPropertyName": "",
                "dPropertyName": "tab_m_suspect",
                "sourceTargetEntities": [
                    {
                        "linkedRelationType": "suspect_case_relation",
                        "onlyFromRel": true,
                        "targetEntityType": "",
                        "description": "嫌疑人身份证",
                        "sPropertyName": "suspect_ssn_name",
                        "sPropertyId": "pic_case_no"
                    }
                ]
            }
        ],
        "indexPropertyMappings": [
            {
                "needRelation": false,
                "description": "案件标识",
                "sPropertyName": "case_no",
                "dPropertyName": "d_case_number",
                "sourceTargetEntities": []
            },
            {
                "needRelation": true,
                "description": "身份证号码",
                "sPropertyName": "",
                "dPropertyName": "d_card_sid",
                "sourceTargetEntities": [
                    {
                        "linkedRelationType": "suspect_case_relation",
                        "onlyFromRel": true,
                        "targetEntityType": "",
                        "description": "嫌疑人身份证",
                        "sPropertyName": "suspect_ssn_id",
                        "sPropertyId": "pic_case_no"
                    }
                ]
            },
            {
                "needRelation": false,
                "description": "办案人联系电话",
                "sPropertyName": "case_operator_tel",
                "dPropertyName": "d_case_number",
                "sourceTargetEntities": []
            },
            {
                "needRelation": false,
                "description": "地址",
                "sPropertyName": "case_detail_address",
                "dPropertyName": "d_addr_addr",
                "sourceTargetEntities": []
            },
            {
                "needRelation": false,
                "description": "所属社区",
                "sPropertyName": "case_belang_community",
                "dPropertyName": "d_addr_addr",
                "sourceTargetEntities": []
            },
            {
                "needRelation": false,
                "description": "发案地域",
                "sPropertyName": "case_area",
                "dPropertyName": "d_addr_addr",
                "sourceTargetEntities": []
            },
            {
                "needRelation": false,
                "description": "发案处所",
                "sPropertyName": "case_station",
                "dPropertyName": "d_addr_addr",
                "sourceTargetEntities": []
            }
        ]
    },
    {
        "docType": "ADDRESS",
        "createTime": ISODate("2017-12-15T17:59:49.106+08:00"),
        "lastModifyTime": ISODate("2017-12-16T17:59:49.106+08:00"),
        "elpModel": "standard_model",
        "elpType": "location",
        "key": "location_address",
        "docCollectionName": "doc_card_index_info",
        "propertyMappings": [
            {
                "needRelation": false,
                "description": "地址标识",
                "sPropertyName": "location_address",
                "dPropertyName": "tab_s_location_id",
                "sourceTargetEntities": []
            },
            {
                "needRelation": false,
                "description": "地址名称",
                "sPropertyName": "location_address",
                "dPropertyName": "tab_s_address",
                "sourceTargetEntities": []
            },
            {
                "needRelation": false,
                "description": "二级搜索地址",
                "sPropertyName": "location_address",
                "dPropertyName": "addr_region",
                "sourceTargetEntities": []
            },
            {
                "needRelation": false,
                "description": "地址经度",
                "sPropertyName": "location_longitude",
                "dPropertyName": "tab_s_locationLon",
                "sourceTargetEntities": []
            },
            {
                "needRelation": false,
                "description": "地址纬度",
                "sPropertyName": "location_latitude",
                "dPropertyName": "tab_s_locationLat",
                "sourceTargetEntities": []
            },
            {
                "needRelation": false,
                "description": "地址所属行政区域",
                "sPropertyName": "location_address",
                "dPropertyName": "tab_s_belongArea",
                "sourceTargetEntities": []
            }
        ],
        "indexPropertyMappings": [
            {
                "needRelation": false,
                "description": "地址",
                "sPropertyName": "location_address",
                "dPropertyName": "d_addr_addr"
            }
        ]
    }
])