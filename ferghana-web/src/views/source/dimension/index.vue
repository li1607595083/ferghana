<template>
  <div class="app-container">
    <el-form :model="queryParams" ref="queryForm" :inline="true" label-width="68px">
      <el-form-item label="维度表名称" prop="dimensionNameZH" label-width="90px">
        <el-input v-model="queryParams.dimensionNameZH" placeholder="请输入维度表名称" clearable size="small"
          @keyup.enter.native="handleQuery" />
      </el-form-item>
      <el-form-item label="维度表名" prop="dimensionName">
        <el-input v-model="queryParams.dimensionName" placeholder="请输入维度表名" clearable size="small"
          @keyup.enter.native="handleQuery" />
      </el-form-item>
      <el-form-item label="连接器类型" prop="connectorType" label-width="90px">
        <el-select v-model="queryParams.connectorType" placeholder="请选择连接器类型" clearable size="small">
          <el-option v-for="dict in connectorTypeOptions" :key="dict.dictValue" :label="dict.dictLabel"
            :value="dict.dictValue" />
        </el-select>
      </el-form-item>
      <el-form-item label="描述" prop="description">
        <el-input v-model="queryParams.description" placeholder="请输入描述" clearable size="small"
          @keyup.enter.native="handleQuery" />
      </el-form-item>
      <el-form-item>
        <el-button type="primary" icon="el-icon-search" size="mini" @click="handleQuery">搜索</el-button>
        <el-button icon="el-icon-refresh" size="mini" @click="resetQuery">重置</el-button>
      </el-form-item>
    </el-form>

    <el-row :gutter="10" class="mb8">
      <el-col :span="1.5">
        <el-button type="primary" icon="el-icon-plus" size="mini" @click="handleAdd">新增
        </el-button>
      </el-col>
      <el-col :span="1.5">
        <el-button type="primary" icon="el-icon-edit" size="mini" :disabled="single" @click="handleUpdate">修改
        </el-button>
      </el-col>
      <el-col :span="1.5">
        <el-button type="primary" icon="el-icon-delete" size="mini" :disabled="multiple" @click="handleDelete">删除
        </el-button>
      </el-col>
    </el-row>

    <el-table v-loading="loading" :data="tableList" @selection-change="handleSelectionChange">
      <el-table-column type="selection" width="55" align="center" />
      <el-table-column label="序号" width="55" align="center" type="index" />
      <el-table-column label="维表中文名" align="center" prop="dimensionNameZH" />
      <el-table-column label="维表英文名" align="center" prop="dimensionName" />
      <el-table-column label="连接器类型" align="center" prop="connectorType" :formatter="connectorTypeFormat" />
      <el-table-column label="创建时间" align="center" prop="createTime" width="180">
        <template slot-scope="scope">
          <span>{{ parseTime(scope.row.createTime) }}</span>
        </template>
      </el-table-column>
      <el-table-column label="修改时间" align="center" prop="modifyTime" width="180">
        <template slot-scope="scope">
          <span>{{ parseTime(scope.row.modifyTime) }}</span>
        </template>
      </el-table-column>
      <el-table-column label="操作" align="center" class-name="small-padding fixed-width">
        <template slot-scope="scope">
          <el-button size="mini" type="text" icon="el-icon-view" @click="handleDetail(scope.row)">详情
          </el-button>
          <el-button size="mini" type="text" icon="el-icon-edit" @click="handleUpdate(scope.row)">修改
          </el-button>
          <el-button size="mini" type="text" icon="el-icon-delete" @click="handleDelete(scope.row)">删除
          </el-button>
        </template>
      </el-table-column>
    </el-table>

    <pagination v-show="total>0" :total="total" :page.sync="queryParams.pageNum" :limit.sync="queryParams.pageSize"
      @pagination="getList" />

    <!-- 添加或修改数据维对话框 -->
    <el-dialog :title="title" :visible.sync="open" width="1050px" :close-on-click-modal="false" class="el-col-24">
      <el-form ref="form" :model="form" :rules="rules" label-width="100px" class="el-col-24">
        <el-form-item label="维表中文名" prop="dimensionNameZH" class="el-col-12">
          <el-input v-model="form.dimensionNameZH" placeholder="请输入维度表名称" :disabled="detailViem" />
        </el-form-item>
        <el-form-item label="维表英文名" prop="dimensionName" v-show="jdbcAndHbaseAndes" class="el-col-12">
          <el-input v-model="form.dimensionName" placeholder="请输入维度表名" :disabled="detailViem" />
        </el-form-item>
        <el-form-item label="连接器类型" prop="connectorType" class="el-col-12">
          <el-select v-model="form.connectorType" style="width: 100%" @change="changeConnOptionsSelect"
            :disabled="detailViem">
            <el-option v-for="dict in connectorTypeOptions" :key="dict.dictValue" :label="dict.dictLabel"
              :value="dict.dictValue" />
          </el-select>
        </el-form-item>
        <el-form-item label="数据来源" prop="dataSource" class="el-col-12">
          <el-input v-model="form.dataSource" placeholder="请输入数据来源" :disabled="detailViem" />
        </el-form-item>
        <el-form-item label="描述" prop="description" class="el-col-12">
          <el-input v-model="form.description" placeholder="请输入描述" :disabled="detailViem" />
        </el-form-item>
        <el-form-item label="集群名" prop="clusterName" class="el-col-12" v-show="redisItem">
          <el-input v-model="form.clusterName" placeholder="请输入集群名" :disabled="detailViem" />
        </el-form-item>
        <el-form-item label="地址" prop="redisAddress" class="el-col-12" v-show="redisItem">
          <el-input v-model="form.redisAddress" placeholder="master:6379,slave:6379" :disabled="detailViem" />
        </el-form-item>

        <el-form-item label="URL地址" prop="jdbcUrlAddress" class="el-col-12" v-show="jdbcItem">
          <el-input v-model="form.jdbcUrlAddress" placeholder="请输入URL地址" :disabled="detailViem" />
        </el-form-item>
        <el-form-item label="驱动类" prop="jdbcDrive" class="el-col-12" v-show="jdbcItem">
          <el-input v-model="form.jdbcDrive" placeholder="请输入驱动类" :disabled="detailViem" />
        </el-form-item>
        <el-form-item label="用户名" prop="jdbcUserName" class="el-col-12" v-show="jdbcItem">
          <el-input v-model="form.jdbcUserName" placeholder="请输入用户名" :disabled="detailViem" />
        </el-form-item>
        <el-form-item label="用户密码" prop="jdbcUserPwd" class="el-col-12" v-show="jdbcItem">
          <el-input v-model="form.jdbcUserPwd" placeholder="请输入用户密码" :disabled="detailViem" />
        </el-form-item>

        <el-form-item label="ES地址" prop="esUrlAddress" class="el-col-12" v-show="esItem">
          <el-input v-model="form.esUrlAddress" placeholder="请输入ES地址" :disabled="detailViem" />
        </el-form-item>
        <el-form-item label="索引" prop="esIndex" class="el-col-12" v-show="esItem">
          <el-input v-model="form.esIndex" placeholder="请输入索引" :disabled="detailViem" />
        </el-form-item>

        <!-- 动态增加项目 -->
        <!-- 不止一项，用div包裹起来 -->
        <div v-for="(item, index) in form.jdbcDynamicItem" :key="'B'+index" v-show="jdbcItem" class="el-col-24">
          <el-form-item label="schema" class="el-col-6" :prop="'jdbcDynamicItem.' + index + '.jdbcKey'"
            :rules="rules.jdbcDynamicItem.jdbcKey">
            <el-input v-model="item.jdbcKey" @change="schemaDefineChange('jdbc',index)" placeholder="请输入字段"
              style="width: 200px" :disabled="item.isUsed === '1'" />
          </el-form-item>
          <el-form-item class="el-col-5 elementStyle" :prop="'jdbcDynamicItem.' + index + '.jdbcType'"
            :rules="rules.jdbcDynamicItem.jdbcType">
            <el-select v-model="item.jdbcType" placeholder="请选择数据类型" clearable style="width: 190px"
              :disabled="item.isUsed === '1'">
              <el-option v-for="dict in sysDataBaseTypes" :key="dict.dictValue" :label="dict.dictLabel"
                :value="dict.dictValue" />
            </el-select>
          </el-form-item>
          <el-form-item class="el-col-5" :prop="'jdbcDynamicItem.' + index + '.schemaFieldName'"
            :rules="rules.jdbcDynamicItem.schemaFieldName">
            <el-input v-model="item.schemaFieldName" placeholder="请输入中文名" style="width: 200px" :disabled="detailViem" />
          </el-form-item>
          <el-form-item class="el-col-2 elementStyle" :prop="'jdbcDynamicItem.' + index + '.primaryKey'">
            <el-input v-model="item.primaryKey" :disabled="detailViem" v-show="false" />
            <el-button @click="primaryKeyCheck(index)" :ref="'ref' + index" :id="'ref' + index"
              :disabled="item.isUsed === '1'" style="padding: 10px;"
              :style="{backgroundColor:item.primaryKey !== '' ? '#1890ff' : '#fff',color:item.primaryKey !== '' ? '#fff' : '#C0C4CC',}">
              主键
            </el-button>
          </el-form-item>
          <el-form-item class="el-col-6">
            <span>
              <el-button @click="jdbcAddItem" :disabled="detailViem">
                <i class="el-icon-plus" />
              </el-button>
              <el-button @click="jdbcDeleteItem(item, index)" :disabled="item.isUsed === '1'">
                <i class="el-icon-minus" />
              </el-button>
            </span>
          </el-form-item>
        </div>

        <el-form-item label="ZK地址" prop="zookeeperAddress" class="el-col-12" v-show="ZKItem">
          <el-input v-model="form.zookeeperAddress" placeholder="master:2181,slave:2181" :disabled="detailViem" />
        </el-form-item>
        <el-form-item label="rowkey" prop="rowkey" class="el-col-24" v-show="ZKItem">
          <el-input v-model="form.rowkey" placeholder="请输入rowkey" :disabled="detailViem" />
        </el-form-item>

        <div v-for="(item, index) in form.esDynamicItem" :key="'E'+index" v-show="esItem" class="el-col-24">
          <el-form-item label="schema" class="el-col-6" :prop="'esDynamicItem.' + index + '.esKey'"
            :rules="rules.esDynamicItem.esKey">
            <el-input v-model="item.esKey" @change="schemaDefineChange('es',index)" placeholder="请输入字段"
              style="width: 200px" :disabled="item.isUsed === '1'" />
          </el-form-item>
          <el-form-item class="el-col-5 elementStyle" :prop="'esDynamicItem.' + index + '.esType'"
            :rules="rules.esDynamicItem.esType">
            <el-select v-model="item.esType" placeholder="请选择数据类型" clearable style="width: 190px"
              :disabled="item.isUsed === '1'">
              <el-option v-for="dict in sysDataBaseTypes" :key="dict.dictValue" :label="dict.dictLabel"
                :value="dict.dictValue" />
            </el-select>
          </el-form-item>
          <el-form-item class="el-col-5" :prop="'esDynamicItem.' + index + '.schemaFieldName'"
            :rules="rules.esDynamicItem.schemaFieldName">
            <el-input v-model="item.schemaFieldName" placeholder="请输入中文名" style="width: 200px" :disabled="detailViem" />
          </el-form-item>
          <el-form-item class="el-col-2 elementStyle" :prop="'jdbcDynamicItem.' + index + '.primaryKey'">
            <el-input v-model="item.primaryKey" :disabled="detailViem" v-show="false" />
            <el-button @click="esprimaryKeyCheck(index)" :ref="'ref' + index" :id="'ref' + index"
              :disabled="item.isUsed === '1'" style="padding: 10px;"
              :style="{backgroundColor:item.primaryKey !== '' ? '#1890ff' : '#fff',color:item.primaryKey !== '' ? '#fff' : '#C0C4CC',}">
              主键
            </el-button>
          </el-form-item>
          <el-form-item class="el-col-6">
            <span>
              <el-button @click="esAddItem" :disabled="detailViem">
                <i class="el-icon-plus" />
              </el-button>
              <el-button @click="esDeleteItem(item, index)" :disabled="item.isUsed === '1'">
                <i class="el-icon-minus" />
              </el-button>
            </span>
          </el-form-item>
        </div>

        <!--                         rules.hbaseItem.div1.hbaseColumnItem.hbaseColumnFamily -->
        <!--        [{ required: true, message: '列族不能为空', trigger: ['blur', 'change']}]-->
        <!-- 动态增加族 -->
        <!-- 不止一项，用div包裹起来 -->
        <div v-for="(item, index) in form.hbaseItem" :key="'C'+index" v-show="ZKItem" class="el-col-24">
          <div v-for="(item1, index1) in form.hbaseItem[index].div1.hbaseColumnItem" :key="'D'+index1" class="el-col-24"
            :prop="'hbaseItem.' + index + '.div11'">
            <el-form-item label="列族" class="el-col-8"
              :prop="'hbaseItem['+index+'].div1.hbaseColumnItem.' + index1 + '.hbaseColumnFamily'"
              :rules="rules.hbaseItem.div1.hbaseColumnItem.hbaseColumnFamily">
              <el-input v-model="item1.hbaseColumnFamily" placeholder="请输入列族" style="width: 200px"
                :disabled="detailViem" />
            </el-form-item>
            <el-form-item class="el-col-16">
              <el-button @click="hbaseAddItem(item,index)" :disabled="detailViem">
                <i class="el-icon-plus" />增加列族
              </el-button>
              <el-button @click="hbaseDeleteItem(item, index)" :disabled="detailViem">
                <i class="el-icon-minus" />删除列族
              </el-button>
            </el-form-item>
          </div>

          <!-- 动态增加列 -->
          <!-- 不止一项，用div包裹起来 -->
          <div v-for="(item2, index2) in form.hbaseItem[index].div2.hbaseDynamicItem" :key="'E'+index2"
            class="el-col-24" :prop="'hbaseItem.' + index + '.div22'">
            <div>
              <el-form-item label="列名" class="el-col-6"
                :prop="'hbaseItem['+index+'].div2.hbaseDynamicItem.' + index2 + '.hbaseKey'"
                :rules="rules.hbaseItem.div2.hbaseDynamicItem.hbaseKey">
                <el-input v-model="item2.hbaseKey" placeholder="请输入字段" style="width: 200px" :disabled="detailViem" />
              </el-form-item>
              <el-form-item class="el-col-6 elementStyle"
                :prop="'hbaseItem['+index+'].div2.hbaseDynamicItem.' + index2 + '.hbaseType'"
                :rules="rules.hbaseItem.div2.hbaseDynamicItem.hbaseType">
                <el-select v-model="item2.hbaseType" placeholder="请选择数据类型" clearable style="width: 200px"
                  :disabled="detailViem">
                  <el-option v-for="dict in sysDataBaseTypes" :key="dict.dictValue" :label="dict.dictLabel"
                    :value="dict.dictValue" />
                </el-select>
              </el-form-item>
              <el-form-item class="el-col-6"
                :prop="'hbaseItem['+index+'].div2.hbaseDynamicItem.' + index2 + '.schemaFieldName'"
                :rules="rules.hbaseItem.div2.hbaseDynamicItem.schemaFieldName">
                <el-input v-model="item2.schemaFieldName" placeholder="请输入中文名" style="width: 200px"
                  :disabled="detailViem" />
              </el-form-item>
            </div>
            <div>
              <el-form-item class="el-col-6">
                <span>
                  <el-button @click="hbaseAddDynamicItem(index, item2, index2)" :disabled="detailViem">
                    <i class="el-icon-plus" />
                  </el-button>
                  <el-button @click="hbaseDeleteDynamicItem(index, item2, index2)" :disabled="detailViem">
                    <i class="el-icon-minus" />
                  </el-button>
                </span>
              </el-form-item>
            </div>
          </div>
        </div>


      </el-form>
      <div slot="footer" class="dialog-footer">
        <el-button type="primary" @click="submitForm" :disabled="detailViem">确 定</el-button>
        <el-button @click="cancel">取 消</el-button>
      </div>
    </el-dialog>
  </div>
</template>

<script>
  import {
    listDimension,
    getDimension,
    delDimension,
    addDimension,
    updateDimension,
    exportDimension
  } from "@/api/source/dimension";
  import {
    isAddressPort,
    isLegitimateName,
    isHbaseName,
    unContainSpace
  } from "@/utils/validate.js";

  export default {
    name: "Dimension",
    data() {
      return {
        // 遮罩层
        loading: true,
        // 选中数组
        ids: [],
        // 非单个禁用
        single: true,
        // 非多个禁用
        multiple: true,
        // 总条数
        total: 0,
        // 数据维表格数据
        tableList: [],
        // 弹出层标题
        title: "",
        // 是否显示弹出层
        open: false,
        // 连接器类型字典
        connectorTypeOptions: [],
        // redis数据类型字典
        redisDataTypeOptions: [],
        // 默认连接器为redis form.connectorType
        connectorType: "01",
        // 默认redis 的数据类型为 String
        redisDataType: "01",
        // 控制jdbc的值是否可见
        jdbcItem: false,
        // 控制Zookeeper是否可见
        ZKItem: false,
        // 控制redis是否可见
        redisItem: false,
        // 控制es是否可见
        esItem: false,
        // 数据类型
        sysDataBaseTypes: [],
        // 控制标签是否可修改
        detailViem: false,
        // 控制redis的key是否可见
        rediskeyItem: false,
        // 只有jdbc、hbase和es才有英文名
        jdbcAndHbaseAndes: false,
        // 查询参数
        queryParams: {
          pageNum: 1,
          pageSize: 10,
          dimensionName: undefined,
          connectorType: undefined,
          dimensionNameZH: undefined,
          description: undefined,
        },
        // 表单参数
        form: {},
        // 表单校验
        rules: {
          dimensionNameZH: [{
              required: true,
              message: "维度表名称不能为空",
              trigger: "blur"
            },
            {
              validator: unContainSpace,
              message: "维度表名称不能包含空格",
              trigger: "blur"
            }
          ],
          dimensionName: [{
              required: true,
              message: "维表英文名不能为空",
              trigger: "blur"
            },
            {
              validator: isLegitimateName,
              message: "格式：只能包含英文字母、数字、下划线且首字母必须是英文字母",
              trigger: "blur"
            },
            {
              validator: isHbaseName,
              message: "格式：只能包含英文字母、数字、下划线且首字母必须是英文字母,必有：",
              trigger: "blur"
            }
          ],
          connectorType: [{
            required: true,
            message: "连接器类型不能为空",
            trigger: "blur"
          }],
          dataSource: [{
            required: true,
            message: "数据来源不能为空",
            trigger: "blur"
          }],
          redisAddress: [{
            required: true,
            message: "redis地址不能为空",
            trigger: "blur"
          }, ],
          clusterName: [{
            required: true,
            message: "集群名不能为空",
            trigger: "blur"
          }, ],
          jdbcUrlAddress: [{
            required: true,
            message: "地址不能为空",
            trigger: "blur"
          }],
          jdbcDrive: [{
            required: true,
            message: "驱动类不能为空",
            trigger: "blur"
          }],
          jdbcUserName: [{
            required: true,
            message: "用户名不能为空",
            trigger: "blur"
          }],
          jdbcUserPwd: [{
            required: true,
            message: "用户密码不能为空",
            trigger: "blur"
          }],
          esUrlAddress: [{
            required: true,
            message: "ES地址不能为空",
            trigger: "blur"
          }],
          esIndex: [{
              required: true,
              message: "索引不能为空",
              trigger: "blur"
            },
            {
              validator: isLegitimateName,
              message: "格式：只能包含英文字母、数字、下划线且首字母必须是英文字母",
              trigger: "blur"
            }
          ],
          schemaDefine: [{
            required: true,
            message: "字段不能为空",
            trigger: "blur"
          }],
          zookeeperAddress: [{
            required: true,
            message: "ZK地址不能为空",
            trigger: "blur"
          }, ],
          rowkey: [{
              required: true,
              message: "rowkey不能为空",
              trigger: "blur"
            },
            {
              validator: isLegitimateName,
              message: "格式：只能包含英文字母、数字、下划线且首字母必须是英文字母",
              trigger: "blur"
            }
          ],
          jdbcDynamicItem: {
            jdbcKey: [{
                required: true,
                message: "字段不能为空",
                trigger: "blur"
              },
              {
                validator: isLegitimateName,
                message: "格式不合法",
                trigger: "blur"
              }
            ],
            jdbcType: [{
              required: true,
              message: "数据类型不能为空",
              trigger: "blur"
            }],
            schemaFieldName: [{
              required: true,
              message: "中文名不能为空",
              trigger: "blur"
            }]
          },
          esDynamicItem: {
            esKey: [{
                required: true,
                message: "字段不能为空",
                trigger: "blur"
              },
              {
                validator: isLegitimateName,
                message: "格式不合法",
                trigger: "blur"
              }
            ],
            esType: [{
              required: true,
              message: "数据类型不能为空",
              trigger: "blur"
            }],
            schemaFieldName: [{
              required: true,
              message: "中文名不能为空",
              trigger: "blur"
            }]
          },
          // redisDynamicItem: {
          //   redisSchemaKey: [
          //     {required: true, message: "字段不能为空", trigger: "blur"}
          //   ],
          //   redisSchemaField: [
          //     {required: true, message: "数据类型不能为空", trigger: "blur"}
          //   ],
          //   schemaFieldName: [
          //     {required: true, message: "中文名不能为空", trigger: "blur"}
          //   ]
          // },
          hbaseItem: {
            div1: {
              div11: "",
              hbaseColumnItem: {
                hbaseColumnFamily: [{
                    required: true,
                    message: "列族不能为空",
                    trigger: "blur"
                  },
                  {
                    validator: isLegitimateName,
                    message: "格式不合法",
                    trigger: "blur"
                  }
                ]
              }
            },
            div2: {
              div22: "",
              hbaseDynamicItem: {
                hbaseKey: [{
                    required: true,
                    message: "列名不能为空",
                    trigger: "blur"
                  },
                  {
                    validator: isLegitimateName,
                    message: "格式不合法",
                    trigger: "blur"
                  }
                ],
                hbaseType: [{
                  required: true,
                  message: "类型不能为空",
                  trigger: "blur"
                }],
                schemaFieldName: [{
                  required: true,
                  message: "中文名不能为空",
                  trigger: "blur"
                }]
              }
            }
          }
        }
      };
    },
    created() {
      this.getList();
      this.getDicts("sys_dimension_conn_type").then(response => {
        this.connectorTypeOptions = response.data;
      });
      this.getDicts("redis_data_type").then(response => {
        this.redisDataTypeOptions = response.data;
      });
      this.getDicts("sys_data_base_type").then(response => {
        this.sysDataBaseTypes = response.data;
      });
    },
    methods: {
      schemaDefineChange(type, index) {
        if (this.form.jdbcDynamicItem[index].primaryKey !== "" && type === "jdnc") {
          this.form.jdbcDynamicItem[index].primaryKey = this.form.jdbcDynamicItem[index].jdbcKey;
          this.form.jdbcPrimaryKey = this.form.jdbcDynamicItem[index].jdbcKey;
        }
        if (this.form.esDynamicItem[index].primaryKey !== "" && type === "es") {
          this.form.esDynamicItem[index].primaryKey = this.form.esDynamicItem[index].esKey;
          this.form.esPrimaryKey = this.form.esDynamicItem[index].esKey;
        }
      },
      // redis增加行
      addRedisDynamicItem() {
        this.form.redisDynamicItem.push({
          redisSchemaKey: "",
          redisSchemaField: "",
          schemaFieldName: ""
        });
      },
      // redis删除行
      deleteRedisDynamicItem(item, index) {
        if (this.form.redisDynamicItem && this.form.redisDynamicItem.length === 1) {
          this.$alert("不能删除所有的字段", {
            type: "error"
          });
          return false;
        }
        this.form.redisDynamicItem.splice(index, 1);
      },
      // jdbc增加行
      jdbcAddItem() {
        this.form.jdbcDynamicItem.push({
          jdbcKey: "",
          jdbcType: "",
          schemaFieldName: "",
          primaryKey: ""
        });
      },
      // jdbc删除行
      jdbcDeleteItem(item, index) {
        if (this.form.jdbcDynamicItem && this.form.jdbcDynamicItem.length === 1) {
          this.$alert("不能删除所有的字段", {
            type: "error"
          });
          return false;
        }
        this.form.jdbcDynamicItem.splice(index, 1);
      },
      // es增加行
      esAddItem() {
        this.form.esDynamicItem.push({
          esKey: "",
          esType: "",
          schemaFieldName: "",
          primaryKey: ""
        });
      },
      // es删除行
      esDeleteItem(item, index) {
        if (this.form.esDynamicItem && this.form.esDynamicItem.length === 1) {
          this.$alert("不能删除所有的字段", {
            type: "error"
          });
          return false;
        }
        this.form.esDynamicItem.splice(index, 1);
      },
      // jdbc主键选择
      primaryKeyCheck(index) {
        // 判断当前字段是否为主键
        if (this.form.jdbcDynamicItem[index].primaryKey === '1')
          this.form.jdbcDynamicItem[index].primaryKey = '';
        // 当前字段不为主键，先设置其他字段不为主键，再设置当前字段为主键
        else {
          for (let i = 0; i < this.form.jdbcDynamicItem.length; i++) {
            this.form.jdbcDynamicItem[i].primaryKey = '';
          }
          this.form.jdbcDynamicItem[index].primaryKey = '1';
          this.form.jdbcPrimaryKey = this.form.jdbcDynamicItem[index].jdbcKey;
        }
        // // 先删除其他的所有的颜色
        // for (let i = 0; i < this.form.jdbcDynamicItem.length; i++) {
        //   let dom = document.querySelector('#ref' + i);
        //   dom.style.backgroundColor = "#ffffff";
        //   dom.style.color = "#C0C4CC";
        //   if (index !== i) {
        //     this.form.jdbcDynamicItem[i].primaryKey = "";
        //   }
        // }
        // // 点击当前这个按钮变背景颜色
        // var dom1 = document.querySelector('#ref' + index);
        // // 如果已经为主键，则先取消
        // if (this.form.jdbcDynamicItem[index].primaryKey !== "") {
        //   dom1.style.backgroundColor = "#ffffff";
        //   this.form.jdbcDynamicItem[index].primaryKey = "";
        // } else {
        //   dom1.style.backgroundColor = "#1890ff";
        //   dom1.style.color = "#ffffff";
        //   // 赋值
        //   if (this.form.jdbcDynamicItem[index].jdbcKey === "") {
        //     this.form.jdbcDynamicItem[index].primaryKey = "1";
        //   } else {
        //     this.form.jdbcDynamicItem[index].primaryKey = this.form.jdbcDynamicItem[index].jdbcKey;
        //     this.form.jdbcPrimaryKey = this.form.jdbcDynamicItem[index].jdbcKey;
        //   }
        // }
      },
      // es主键选择
      esprimaryKeyCheck(index) {
        // 判断当前字段是否为主键
        if (this.form.esDynamicItem[index].primaryKey === '1')
          this.form.esDynamicItem[index].primaryKey = '';
        // 当前字段不为主键，先设置其他字段不为主键，再设置当前字段为主键
        else {
          for (let i = 0; i < this.form.esDynamicItem.length; i++) {
            this.form.esDynamicItem[i].primaryKey = '';
          }
          this.form.esDynamicItem[index].primaryKey = '1';
          this.form.esPrimaryKey = this.form.esDynamicItem[index].esKey;
        }
      },

      // hbase增加列
      hbaseAddDynamicItem(index, item2, index2) {
        this.form.hbaseItem[index].div2.hbaseDynamicItem.push({
          hbaseKey: "",
          schemaFieldName: "",
          hbaseType: ""
        });
      },
      // hbase删除列
      hbaseDeleteDynamicItem(index, item2, index2) {
        if (this.form.hbaseItem[index].div2.hbaseDynamicItem && this.form.hbaseItem[index].div2.hbaseDynamicItem
          .length === 1) {
          this.$alert("不能删除所有的字段", {
            type: "error"
          });
          return false;
        }
        this.form.hbaseItem[index].div2.hbaseDynamicItem.splice(index2, 1);
      },
      // hbase增加行
      hbaseAddItem(item, index) {
        this.form.hbaseItem.push({
          div1: {
            div11: "",
            hbaseColumnItem: [{
              hbaseColumnFamily: ""
            }]
          },
          div2: {
            div22: "",
            hbaseDynamicItem: [{
              hbaseKey: "",
              schemaFieldName: "",
              hbaseType: ""
            }]
          }
        });

      },
      // hbase删除行
      hbaseDeleteItem(item, index) {
        if (this.form.hbaseItem && this.form.hbaseItem.length === 1) {
          this.$alert("不能删除所有的列族", {
            type: "error"
          });
          return false;
        }
        this.form.hbaseItem.splice(index, 1);
      },
      // 改变连接器类型时，展示对应的字段
      changeConnOptionsSelect() {
        if (this.form.connectorType === "01") {
          this.redisCheck();
        } else if (this.form.connectorType === "02") {
          this.jdbcCheck();
        } else if (this.form.connectorType === "03") {
          this.hbaseCheck();
        } else if (this.form.connectorType === "04") {
          this.elasticsearchCheck();
        } else if (this.form.connectorType === "05") {
          this.elasticsearchCheck();
        }
      },
      // 当前为redis的校验
      redisCheck() {
        this.jdbcItem = false;
        this.jdbcAndHbaseAndes = false;
        this.ZKItem = false;
        this.redisItem = true;
        this.rediskeyItem = false;
        this.esItem = false;
        this.rules.redisAddress[0].required = true;
        this.rules.clusterName[0].required = true;
        this.rules.jdbcUrlAddress[0].required = false;
        this.rules.dimensionName[0].required = false;
        this.rules.dimensionName[1].validator = false;
        this.rules.dimensionName[2].validator = false;
        this.rules.jdbcDrive[0].required = false;
        this.rules.jdbcUserName[0].required = false;
        this.rules.jdbcUserPwd[0].required = false;
        this.rules.jdbcDynamicItem.jdbcKey[0].required = false;
        this.rules.jdbcDynamicItem.jdbcKey[1].required = false;
        this.rules.jdbcDynamicItem.jdbcType[0].required = false;
        this.rules.jdbcDynamicItem.schemaFieldName[0].required = false;
        this.rules.esDynamicItem.esKey[0].required = false;
        this.rules.esDynamicItem.esKey[1].validator = false;
        this.rules.esDynamicItem.esType[0].required = false;
        this.rules.esDynamicItem.schemaFieldName[0].required = false;
        this.rules.esIndex[0].required = false;
        this.rules.esIndex[1].validator = false;
        this.rules.esUrlAddress[0].required = false;
        this.rules.zookeeperAddress[0].required = false;
        this.rules.rowkey[0].required = false;
        this.rules.rowkey[1].required = false;
        this.rules.hbaseItem.div1.hbaseColumnItem.hbaseColumnFamily[0].required = false;
        this.rules.hbaseItem.div1.hbaseColumnItem.hbaseColumnFamily[1].required = false;
        this.rules.hbaseItem.div2.hbaseDynamicItem.hbaseType[0].required = false;
        this.rules.hbaseItem.div2.hbaseDynamicItem.hbaseKey[0].required = false;
        this.rules.hbaseItem.div2.hbaseDynamicItem.hbaseKey[1].required = false;
        this.rules.hbaseItem.div2.hbaseDynamicItem.schemaFieldName[0].required = false;

      },
      // 当前为jdbc的校验
      jdbcCheck() {
        this.jdbcItem = true;
        this.jdbcAndHbaseAndes = true;
        this.ZKItem = false;
        this.redisItem = false;
        this.rediskeyItem = false;
        this.esItem = false;
        // this.rules.redisKey[0].required = false;
        // this.rules.redisDataType[0].required = false;
        this.rules.redisAddress[0].required = false;
        this.rules.clusterName[0].required = false;
        this.rules.dimensionName[0].required = true;
        this.rules.dimensionName[1].validator = isLegitimateName;
        this.rules.dimensionName[2].validator = false;
        // this.rules.redisDynamicItem.redisSchemaKey[0].required = false;
        // this.rules.redisDynamicItem.redisSchemaField[0].required = false;
        // this.rules.redisDynamicItem.schemaFieldName[0].required = false;
        this.rules.jdbcUrlAddress[0].required = true;
        this.rules.jdbcDrive[0].required = true;
        this.rules.jdbcUserName[0].required = true;
        this.rules.jdbcUserPwd[0].required = true;
        this.rules.zookeeperAddress[0].required = false;
        this.rules.jdbcDynamicItem.jdbcKey[1].required = isLegitimateName;
        this.rules.jdbcDynamicItem.jdbcKey[0].required = true;
        this.rules.jdbcDynamicItem.jdbcType[0].required = true;
        this.rules.jdbcDynamicItem.schemaFieldName[0].required = true;
        this.rules.esDynamicItem.esKey[0].required = false;
        this.rules.esDynamicItem.esKey[1].validator = false;
        this.rules.esDynamicItem.esType[0].required = false;
        this.rules.esDynamicItem.schemaFieldName[0].required = false;
        this.rules.esIndex[0].required = false;
        this.rules.esIndex[1].validator = false;
        this.rules.esUrlAddress[0].required = false;
        this.rules.rowkey[0].required = false;
        this.rules.rowkey[1].required = false;
        this.rules.hbaseItem.div1.hbaseColumnItem.hbaseColumnFamily[0].required = false;
        this.rules.hbaseItem.div1.hbaseColumnItem.hbaseColumnFamily[1].required = false;
        this.rules.hbaseItem.div2.hbaseDynamicItem.hbaseType[0].required = false;
        this.rules.hbaseItem.div2.hbaseDynamicItem.hbaseKey[0].required = false;
        this.rules.hbaseItem.div2.hbaseDynamicItem.hbaseKey[1].required = false;
        this.rules.hbaseItem.div2.hbaseDynamicItem.schemaFieldName[0].required = false;
      },
      // 当前为hbase的校验
      hbaseCheck() {
        this.jdbcItem = false;
        this.ZKItem = true;
        this.jdbcAndHbaseAndes = true;
        this.redisItem = false;
        this.esItem = false;
        this.rediskeyItem = false;
        // this.rules.redisKey[0].required = false;
        // this.rules.redisDataType[0].required = false;
        this.rules.redisAddress[0].required = false;
        this.rules.clusterName[0].required = false;
        // this.rules.redisDynamicItem.redisSchemaKey[0].required = false;
        // this.rules.redisDynamicItem.redisSchemaField[0].required = false;
        // this.rules.redisDynamicItem.schemaFieldName[0].required = false;
        this.rules.jdbcUrlAddress[0].required = false;
        this.rules.jdbcDrive[0].required = false;
        this.rules.jdbcUserName[0].required = false;
        this.rules.jdbcUserPwd[0].required = false;
        this.rules.zookeeperAddress[0].required = true;
        this.rules.jdbcDynamicItem.jdbcKey[0].required = false;
        this.rules.jdbcDynamicItem.jdbcKey[1].required = false;
        this.rules.jdbcDynamicItem.jdbcType[0].required = false;
        this.rules.jdbcDynamicItem.schemaFieldName[0].required = false;
        this.rules.esDynamicItem.esKey[0].required = false;
        this.rules.esDynamicItem.esKey[1].validator = false;
        this.rules.esDynamicItem.esType[0].required = false;
        this.rules.esDynamicItem.schemaFieldName[0].required = false;
        this.rules.esIndex[0].required = false;
        this.rules.esIndex[1].validator = false;
        this.rules.esUrlAddress[0].required = false;
        this.rules.rowkey[0].required = true;
        this.rules.rowkey[1].required = isLegitimateName;
        this.rules.dimensionName[0].required = true;
        this.rules.dimensionName[1].validator = false;
        this.rules.dimensionName[2].validator = isHbaseName;
        this.rules.hbaseItem.div1.hbaseColumnItem.hbaseColumnFamily[0].required = true;
        this.rules.hbaseItem.div1.hbaseColumnItem.hbaseColumnFamily[1].required = isLegitimateName;
        this.rules.hbaseItem.div2.hbaseDynamicItem.hbaseType[0].required = true;
        this.rules.hbaseItem.div2.hbaseDynamicItem.hbaseKey[0].required = true;
        this.rules.hbaseItem.div2.hbaseDynamicItem.hbaseKey[1].required = isLegitimateName;
        this.rules.hbaseItem.div2.hbaseDynamicItem.schemaFieldName[0].required = true;
      },
      // 当前为elasticsearch-6.X、elasticsearch-7.X的校验
      elasticsearchCheck() {
        this.jdbcItem = false;
        this.jdbcAndHbaseAndes = true;
        this.ZKItem = false;
        this.redisItem = false;
        this.rediskeyItem = false;
        this.esItem = true;
        this.rules.redisAddress[0].required = false;
        this.rules.clusterName[0].required = false;
        this.rules.jdbcUrlAddress[0].required = false;
        this.rules.dimensionName[0].required = true;
        this.rules.dimensionName[1].validator = isLegitimateName;
        this.rules.dimensionName[2].validator = false;
        this.rules.jdbcDrive[0].required = false;
        this.rules.jdbcUserName[0].required = false;
        this.rules.jdbcUserPwd[0].required = false;
        this.rules.jdbcDynamicItem.jdbcKey[0].required = false;
        this.rules.jdbcDynamicItem.jdbcKey[1].required = false;
        this.rules.jdbcDynamicItem.jdbcType[0].required = false;
        this.rules.jdbcDynamicItem.schemaFieldName[0].required = false;
        this.rules.esDynamicItem.esKey[0].required = true;
        this.rules.esDynamicItem.esKey[1].validator = isLegitimateName;
        this.rules.esDynamicItem.esType[0].required = true;
        this.rules.esDynamicItem.schemaFieldName[0].required = true;
        this.rules.esIndex[0].required = true;
        this.rules.esIndex[1].validator = isLegitimateName;
        this.rules.esUrlAddress[0].required = true;
        this.rules.zookeeperAddress[0].required = false;
        this.rules.rowkey[0].required = false;
        this.rules.rowkey[1].required = false;
        this.rules.hbaseItem.div1.hbaseColumnItem.hbaseColumnFamily[0].required = false;
        this.rules.hbaseItem.div1.hbaseColumnItem.hbaseColumnFamily[1].required = false;
        this.rules.hbaseItem.div2.hbaseDynamicItem.hbaseType[0].required = false;
        this.rules.hbaseItem.div2.hbaseDynamicItem.hbaseKey[0].required = false;
        this.rules.hbaseItem.div2.hbaseDynamicItem.hbaseKey[1].required = false;
        this.rules.hbaseItem.div2.hbaseDynamicItem.schemaFieldName[0].required = false;
      },
      // 切换redis的数据类型时，展示key
      changeRedisDataTypeSelect() {
        if (this.form.redisDataType === '01') {
          this.rediskeyItem = false;
          this.rules.redisKey[0].required = false;
        } else if (this.form.redisDataType === '02') {
          this.rediskeyItem = true;
          this.rules.redisKey[0].required = true;
        }
      },
      /** 查询数据维列表 */
      getList() {
        this.loading = true;
        listDimension(this.queryParams).then(response => {
          this.tableList = response.rows;
          this.total = response.total;
          this.loading = false;
        });
      },
      // 连接器类型字典翻译
      connectorTypeFormat(row, column) {
        return this.selectDictLabel(this.connectorTypeOptions, row.connectorType);
      },
      // redis数据类型字典翻译
      redisDataTypeFormat(row, column) {
        return this.selectDictLabel(this.redisDataTypeOptions, row.redisDataType);
      },
      // 取消按钮
      cancel() {
        // this.clearKeyColor();
        this.open = false;
        this.reset();
      },
      // 清空jdbc主键颜色
      clearKeyColor() {
        for (let i = 0; i < this.form.jdbcDynamicItem.length; i++) {
          if (this.form.jdbcDynamicItem[i].primaryKey !== "") {
            let dom1 = document.querySelector('#ref' + i);
            dom1.style.backgroundColor = "#ffffff";
            dom1.style.color = "#C0C4CC";
          }
        }
      },
      // 表单重置
      reset() {
        this.form = {
          dimensionId: undefined,
          dimensionName: undefined,
          connectorType: undefined,
          dataSource: undefined,
          description: undefined,
          redisAddress: undefined,
          clusterName: undefined,
          redisDataType: undefined,
          jdbcUrlAddress: undefined,
          jdbcDrive: undefined,
          jdbcUserName: undefined,
          jdbcUserPwd: undefined,
          jdbcPrimaryKey: undefined,
          esUrlAddress: undefined,
          esIndex: undefined,
          esPrimaryKey: undefined,
          schemaDefine: undefined,
          redisSchemaDefine: undefined,
          zookeeperAddress: undefined,
          rowkey: undefined,
          createTime: undefined,
          modifyTime: undefined,
          hbaseSchemaDefine: undefined,
          dimensionNameZH: undefined,
          jdbcDynamicItem: [{
            jdbcKey: "",
            jdbcType: "",
            schemaFieldName: "",
            primaryKey: ""
          }],
          esDynamicItem: [{
            esKey: "",
            esType: "",
            schemaFieldName: "",
            primaryKey: ""
          }],
          redisDynamicItem: [{
            redisSchemaKey: "",
            redisSchemaField: "",
            schemaFieldName: ""
          }],
          hbaseItem: [{
            div1: {
              div11: "",
              hbaseColumnItem: [{
                hbaseColumnFamily: ""
              }]
            },
            div2: {
              div22: "",
              hbaseDynamicItem: [{
                hbaseKey: "",
                schemaFieldName: "",
                hbaseType: ""
              }]
            }
          }]
        };
        this.resetForm("form");
        this.form.connectorType = "01";
        this.form.redisDataType = "01";
      },
      /** 搜索按钮操作 */
      handleQuery() {
        this.queryParams.pageNum = 1;
        this.getList();
      },
      /** 重置按钮操作 */
      resetQuery() {
        this.resetForm("queryForm");
        this.handleQuery();
      },
      // 多选框选中数据
      handleSelectionChange(selection) {
        this.ids = selection.map(item => item.dimensionId)
        this.single = selection.length !== 1
        this.multiple = !selection.length
      },
      /** 新增按钮操作 */
      handleAdd() {
        this.reset();
        this.open = true;
        this.title = "添加数据维表";
        this.detailViem = false;
        // 初始新增时，数据类型为redis时，取消jdbc和ZK的信息
        if (this.form.connectorType === '01') {
          this.jdbcItem = false;
          this.ZKItem = false;
          this.redisItem = true;
          this.esItem = false;
          this.redisCheck();
        }
      },
      /** 详情按钮操作 */
      handleDetail(row) {
        this.detailViem = true;
        // this.reset();
        const dimensionId = row.dimensionId || this.ids;
        getDimension(dimensionId).then(response => {
          this.form = response.data;
          this.open = true;
          this.detailViem = true;
          this.showSubmitForm = false;
          this.title = "查看数据维表";
          if (this.form.connectorType === "01") {
            this.redisCheck();
          } else if (this.form.connectorType === "02") {
            this.jdbcCheck();
          } else if (this.form.connectorType === "03") {
            this.hbaseCheck();
          } else if (this.form.connectorType === "04") {
            this.elasticsearchCheck();
          } else if (this.form.connectorType === "05") {
            this.elasticsearchCheck();
          }
          // this.$nextTick(function() {
          //   if(this.form.jdbcDynamicItem != null || this.form.jdbcDynamicItem != ""){
          //     for (let i = 0; i < this.form.jdbcDynamicItem.length; i++) {
          //       if (this.form.jdbcDynamicItem[i].primaryKey !== "") {
          //         let dom1 = document.querySelector('#ref' + i);
          //         dom1.style.backgroundColor = "#1890ff";
          //         dom1.style.color = "#ffffff";
          //       }
          //     }
          //   }
          // })
        });
      },
      /** 修改按钮操作 */
      handleUpdate(row) {
        // this.reset();
        this.detailViem = false;
        const dimensionId = row.dimensionId || this.ids;
        getDimension(dimensionId).then(response => {
          this.form = response.data;
          if (this.form.connectorType === "01") {
            this.redisCheck();
          } else if (this.form.connectorType === "02") {
            this.jdbcCheck();
          } else if (this.form.connectorType === "03") {
            this.hbaseCheck();
          } else if (this.form.connectorType === "04") {
            this.elasticsearchCheck();
          } else if (this.form.connectorType === "05") {
            this.elasticsearchCheck();
          }
          this.open = true;
          this.title = "修改数据维表";

          // this.$nextTick(function() {
          //   if(this.form.jdbcDynamicItem != null || this.form.jdbcDynamicItem != ""){
          //     for (let i = 0; i < this.form.jdbcDynamicItem.length; i++) {
          //       if (this.form.jdbcDynamicItem[i].primaryKey !== "") {
          //         let dom1 = document.querySelector('#ref' + i);
          //         dom1.style.backgroundColor = "#1890ff";
          //         dom1.style.color = "#ffffff";
          //       }
          //     }
          //   }
          // })
        });
      },

      /** 提交按钮 */
      submitForm: function() {
        this.$refs["form"].validate(valid => {
          if (valid) {
            // this.clearKeyColor();
            if (this.form.dimensionId !== undefined) {
              if (this.form.connectorType === "01") {
                this.form.redisSchemaDefine = JSON.stringify(this.form.redisDynamicItem);
                this.form.dimensionName = "";
              }
              if (this.form.connectorType === "02") {
                for (let i = 0; i < this.form.jdbcDynamicItem.length; i++) {
                  this.$delete(this.form.jdbcDynamicItem[i], "primaryKey");
                }
                this.form.schemaDefine = JSON.stringify(this.form.jdbcDynamicItem);
              }
              if (this.form.connectorType === "03") {
                this.form.hbaseSchemaDefine = JSON.stringify(this.form.hbaseItem);
              }
              if (this.form.connectorType === "04") {
                this.form.schemaDefine = JSON.stringify(this.form.esDynamicItem);
              }
              if (this.form.connectorType === "05") {
                this.form.schemaDefine = JSON.stringify(this.form.esDynamicItem);
              }
              console.log("---sss");
              console.log(this.form);
              updateDimension(this.form).then(response => {
                if (response.code === 200) {
                  this.msgSuccess("修改成功");
                  this.open = false;
                  this.getList();
                } else {
                  this.msgError(response.msg);
                }
              });
            } else {
              if (this.form.connectorType === "01") {
                this.form.redisSchemaDefine = JSON.stringify(this.form.redisDynamicItem);
                this.form.dimensionName = "";
              }
              if (this.form.connectorType === "02") {
                // primaryKey
                for (let i = 0; i < this.form.jdbcDynamicItem.length; i++) {
                  this.$delete(this.form.jdbcDynamicItem[i], "primaryKey");
                }
                this.form.schemaDefine = JSON.stringify(this.form.jdbcDynamicItem);
              }
              if (this.form.connectorType === "03") {
                this.form.hbaseSchemaDefine = JSON.stringify(this.form.hbaseItem);
              }
              if (this.form.connectorType === "04") {
                this.form.schemaDefine = JSON.stringify(this.form.esDynamicItem);
              }
              if (this.form.connectorType === "05") {
                this.form.schemaDefine = JSON.stringify(this.form.esDynamicItem);
              }
              // console.log(this.form)
              addDimension(this.form).then(response => {
                if (response.code === 200) {
                  this.msgSuccess("新增成功");
                  this.open = false;
                  this.getList();
                } else {
                  this.msgError(response.msg);
                }
              });
            }
          }
        });
      },

      /** 删除按钮操作 */
      handleDelete(row) {
        const dimensionIds = row.dimensionId || this.ids;
        this.$confirm('是否确认删除该数据维表?', "警告", {
          confirmButtonText: "确定",
          cancelButtonText: "取消",
          type: "warning"
        }).then(function() {
          return delDimension(dimensionIds);
        }).then(() => {
          this.getList();
          this.msgSuccess("删除成功");
        }).catch(function() {});
      },
      /** 导出按钮操作 */
      handleExport() {
        const queryParams = this.queryParams;
        this.$confirm('是否确认导出所有数据维表数据项?', "警告", {
          confirmButtonText: "确定",
          cancelButtonText: "取消",
          type: "warning"
        }).then(function() {
          return exportDimension(queryParams);
        }).then(response => {
          this.download(response.msg);
        }).catch(function() {});
      }
    }
  };
</script>
