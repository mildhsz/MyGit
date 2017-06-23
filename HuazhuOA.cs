using System;
using System.Collections.Generic;
using System.Text;
using System.Xml;
using System.Data;
using System.Data.SqlClient;

namespace Huazhu
{
    public class HuazhuOA
    {
        SqlConnection sqlConn = new SqlConnection();
        SqlCommand sqlComm = new SqlCommand();

        string strConn = string.Empty;

        public HuazhuOA()
        {
            this.Open();
        }

        protected void Open()
        {
            IniFile ini = new IniFile(System.AppDomain.CurrentDomain.BaseDirectory + "huazhu.ini");
            string strServer = ini.IniReadValue("HRM", "Server");
            string strDB = ini.IniReadValue("HRM", "DB");
            strConn = "server="+strServer+";database="+strDB+";User ID=HRPWebSiteDBAdmin;Password=aYr3+e7$K;Integrated Security=False;min pool size=1;max pool size=20000;connect timeout=60;";
            sqlConn.ConnectionString = strConn;
            sqlConn.Open();

            if (sqlConn.State == ConnectionState.Open)
                sqlComm.Connection = sqlConn;

        }

        private string QuoNStr(string strID)
        {
            int i;
            string VarTemp;


            if (strID == null || strID == "Null" || strID == "null")
                return "Null";
            else
            {
                VarTemp = strID;
                i = VarTemp.IndexOf("'");

                while (i > 0)
                {
                    VarTemp = VarTemp.Substring(0, i) + VarTemp.Substring(i);
                    i = VarTemp.IndexOf("'", i + 2);
                }
            }

            return "N'" + VarTemp.ToString() + "'";

        }

        private XmlNode SetXMLNode(XmlDocument doc, string strName, string strValue)
        {
            XmlNode node = doc.CreateNode(XmlNodeType.Element, strName, null);
            node.InnerText = strValue;
            return node;
        }

        private string SetXMLDoc(DataSet dsData)
        {
            XmlDocument xmlDoc = new XmlDocument();

            if (dsData != null && dsData.Tables.Count >= 1)
            {
                XmlNode xmlNode = SetXMLNode(xmlDoc, "DataRows", "");
                xmlDoc.AppendChild(xmlNode);

                foreach (DataRow drRow in dsData.Tables[0].Rows)
                {
                    XmlNode xmlNode1 = SetXMLNode(xmlDoc, "DataRow", "");
                    xmlNode.AppendChild(xmlNode1);
                    for (int i = 0; i < dsData.Tables[0].Columns.Count; i++)
                    {
                        XmlNode xmlNode2 = SetXMLNode(xmlDoc, dsData.Tables[0].Columns[i].ColumnName, drRow[i].ToString());
                        xmlNode.AppendChild(xmlNode2);
                    }
                }
                return xmlDoc.OuterXml;
            }
            else
                return null;
        }

        private string GetNodeValue(DataRow drRow, string strName)
        {
            if (drRow.Table.Columns.IndexOf(strName) >= 0)
                return drRow[strName].ToString();
            else
                return "";
        }

        private string GetNodeInt(DataRow drRow, string strName)
        {
            int intTemp;
            if (drRow.Table.Columns.IndexOf(strName) >= 0)
                if (int.TryParse(drRow[strName].ToString(), out intTemp))
                    return int.Parse(drRow[strName].ToString()).ToString();
                else
                    return "-1";
            else
                return "-1";
        }

        private string GetNodeBool(DataRow drRow, string strName)
        {
            bool intTemp;
            if (drRow.Table.Columns.IndexOf(strName) >= 0)
                if (bool.TryParse(drRow[strName].ToString(), out intTemp))
                    return "true";
                else
                    return "false";
            else
                return "true";
        }

        private double GetNodeNumber(DataRow drRow, string strName)
        {
            double dblTemp;
            if (drRow.Table.Columns.IndexOf(strName) >= 0)
                if (double.TryParse(drRow[strName].ToString(), out dblTemp))
                    return double.Parse(drRow[strName].ToString());
                else
                    return 0;
            else
                return 0;
        }

        private string GetNodeValue(DataRow drRow, string strName, int intLen)
        {
            if (drRow.Table.Columns.IndexOf(strName) >= 0)
                if (drRow[strName].ToString().Length > intLen)
                    return drRow[strName].ToString().Substring(0, intLen);
                else
                    return drRow[strName].ToString();
            else
                return "";
        }

        private bool GetNodeValue(XmlNode node, string strName, bool blnChange)
        {
            if (node.SelectNodes(strName).Count >= 1)
                if (node.SelectNodes(strName)[0].InnerText == "1")
                    return true;
                else
                    return false;
            else
                return false;
        }

        private string GetNodeDate(DataRow drRow, string strName)
        {
            DateTime dtTemp;
            if (drRow.Table.Columns.IndexOf(strName) >= 0)
            {
                if(DateTime.TryParse(drRow[strName].ToString(),out dtTemp))
                    return DateTime.Parse(drRow[strName].ToString()).ToShortDateString();
                else
                    return "null";
            }
            else
                return "null";
        }

        private DataTable GetResultTable(DataTable dtResult, bool blnOK, string strEmpcode, string strResult, string strCode, string strAmount, bool blnNew)
        {
            
            DataTable dtMessage = dtResult;
            if (dtResult == null || dtResult.Columns.Count <= 0)
            {
                DataColumn dcCol1 = new DataColumn("success", typeof(Boolean));
                dtResult.Columns.Add(dcCol1);
                DataColumn dcCol2 = new DataColumn("empcode", typeof(String));
                dtResult.Columns.Add(dcCol2);
                DataColumn dcCol3 = new DataColumn("reason", typeof(String));
                dtResult.Columns.Add(dcCol3);
                DataColumn dcCol4 = new DataColumn("code", typeof(String));
                dtResult.Columns.Add(dcCol4);
                DataColumn dcCol5 = new DataColumn("amount", typeof(String));
                dtResult.Columns.Add(dcCol5);
            }

            if (blnNew)
            {
                DataRow drNew = dtMessage.NewRow();
                drNew["success"] = blnOK;
                drNew["empcode"] = strEmpcode;
                drNew["reason"] = strResult;
                drNew["code"] = strCode;
                drNew["amount"] = strAmount;
                dtMessage.Rows.Add(drNew);
            }
            else
            {
                DataRow drOld = dtMessage.Rows[dtMessage.Rows.Count - 1];
                drOld["success"] = blnOK;
                drOld["empcode"] = strEmpcode;
                drOld["reason"] = strResult;
                drOld["code"] = strCode;
                drOld["amount"] = strAmount;
            }

            return dtMessage;
        }
        private DataTable GetResultTable(DataTable dtResult, bool blnOK, string strEmpcode, string strResult, bool blnNew)
        {
            DataTable dtMessage = dtResult;
            if (dtResult == null || dtResult.Columns.Count <= 0)
            {
                DataColumn dcCol1 = new DataColumn("success", typeof(Boolean));
                dtResult.Columns.Add(dcCol1);
                DataColumn dcCol2 = new DataColumn("empcode", typeof(String));
                dtResult.Columns.Add(dcCol2);
                DataColumn dcCol3 = new DataColumn("reason", typeof(String));
                dtResult.Columns.Add(dcCol3);
            }

            if (blnNew)
            {
                DataRow drNew = dtMessage.NewRow();
                drNew["success"] = blnOK;
                drNew["empcode"] = strEmpcode;
                drNew["reason"] = strResult;
                dtMessage.Rows.Add(drNew);
            }
            else
            {
                DataRow drOld = dtMessage.Rows[dtMessage.Rows.Count - 1];
                drOld["success"] = blnOK;
                drOld["empcode"] = strEmpcode;
                drOld["reason"] = strResult;
            }

            return dtMessage;
        }

        private DataTable GetResultTable(DataTable dtResult, bool blnOK, string strEmpcode, string strChangeid,string strResult, bool blnNew)
        {
            DataTable dtMessage = dtResult;
            if (dtResult == null || dtResult.Columns.Count <= 0)
            {
                DataColumn dcCol1 = new DataColumn("success", typeof(Boolean));
                dtResult.Columns.Add(dcCol1);
                DataColumn dcCol2 = new DataColumn("empcode", typeof(String));
                dtResult.Columns.Add(dcCol2);
                DataColumn dcCol3 = new DataColumn("changeid", typeof(String));
                dtResult.Columns.Add(dcCol3);
                DataColumn dcCol4 = new DataColumn("reason", typeof(String));
                dtResult.Columns.Add(dcCol4);
            }

            if (blnNew)
            {
                DataRow drNew = dtMessage.NewRow();
                drNew["success"] = blnOK;
                drNew["empcode"] = strEmpcode;
                drNew["changeid"] = strChangeid;
                drNew["reason"] = strResult;
                dtMessage.Rows.Add(drNew);
            }
            else
            {
                DataRow drOld = dtMessage.Rows[dtMessage.Rows.Count - 1];
                drOld["success"] = blnOK;
                drOld["empcode"] = strEmpcode;
                drOld["changeid"] = strChangeid;
                drOld["reason"] = strResult;
            }

            return dtMessage;
        }

        public DataTable AddEmployeeData(DataTable dtTable)
        {
            string strSql = string.Empty;
            SqlDataAdapter sqlData = new SqlDataAdapter();
            DataSet dsData = new DataSet();
            
            int iResult = 0;
            DataTable retXML = new DataTable("Results");
            string strSerialNumber = "";

            try
            {
                if (dtTable == null || dtTable.Rows.Count <= 0)
                    return null;
                
                foreach (DataRow drRow in dtTable.Rows)
                {
                    SqlTransaction sqlTran = sqlConn.BeginTransaction();
                    sqlComm.Transaction = sqlTran;
                    sqlComm.CommandText = "select replace(REPLICATE('X',10-len(max(serialnumber)+1))+convert(char,max(serialnumber)+1),'X','0') " +
                        " from employee1 where len(serialnumber)=10";
                    sqlData.SelectCommand = sqlComm;
                    dsData = new DataSet();
                    sqlData.Fill(dsData);
                    if (dsData == null)
                        strSerialNumber = "0000000001";
                    else
                        strSerialNumber = dsData.Tables[0].Rows[0][0].ToString().Substring(0, 10);

                    
                    string strEmpcode = GetNodeValue(drRow, "empcode",20);

                    dsData = new DataSet();
                    sqlComm.CommandText = "select serialnumber from employee1 where empcode='" + strEmpcode + "'";
                    dsData = new DataSet();
                    sqlData.SelectCommand = sqlComm;
                    sqlData.Fill(dsData);

                    if (dsData != null && dsData.Tables.Count > 0 && dsData.Tables[0].Rows.Count > 0)
                    {
                        retXML = GetResultTable(retXML, false, strEmpcode, "Employee exists", true);
                        continue;
                    }
                    //employee1
                    #region
                    string strCname = GetNodeValue(drRow, "c_name", 100);
                    string strEname = GetNodeValue(drRow, "e_name", 100);
                    string strgender = GetNodeValue(drRow, "gender",1);
                    string strppissuecountry = GetNodeValue(drRow, "ppissuecountry",40);

                    string strid = GetNodeValue(drRow, "id",20);
                    string strmarital = GetNodeValue(drRow, "marital",50);
                    string strhiretype = GetNodeValue(drRow, "hiretype",10);
                    string strextreviewdate = GetNodeDate(drRow, "nextreviewdate");
                    string stremail = GetNodeValue(drRow, "email",100);

                    string strpager = GetNodeValue(drRow, "pager",100);
                    string strbusinessphone = GetNodeValue(drRow, "businessphone",20);
                    string strmobile = GetNodeValue(drRow, "mobile", 100);
                    string strraddress = GetNodeValue(drRow, "raddress",100);
                    string strhomephone = GetNodeValue(drRow, "homephone",20);

                    string strrzipcode = GetNodeValue(drRow, "rzipcode",10);
                    string strorigin = GetNodeValue(drRow, "origin",30);
                    string straddress1 = GetNodeValue(drRow, "address1",100);
                    string strdangan = GetNodeValue(drRow, "dangan",50);
                    string stre_sur_name = GetNodeValue(drRow, "e_sur_name",100);

                    string stre_first_name = GetNodeValue(drRow, "e_first_name", 100);
                    string strhuko = GetNodeValue(drRow, "huko",100);
                    string strbloodtype = GetNodeValue(drRow, "bloodtype",2);
                    string strhrstatus = GetNodeValue(drRow, "hrstatus",50);
                    string strbirthday = GetNodeDate(drRow, "birthday");

                    string strIssuecountry = GetNodeValue(drRow, "Issuecountry",10);
                    string strppnumber = GetNodeValue(drRow, "ppnumber",10);
                    string strppexpiredate = GetNodeDate(drRow, "ppexpiredate");
                    string straddress2 = GetNodeValue(drRow, "address2",100);

                    string strquitdate = GetNodeDate(drRow, "quitdate");
                    string strquitnoticedate = GetNodeDate(drRow, "quitnoticedate");
                    string strquittype = GetNodeValue(drRow, "quittype",10);
                    string strquitreason = GetNodeValue(drRow, "quitreason",1000);

                    string strhiredate = GetNodeDate(drRow, "hiredate");
                    string strhrid = "1";
                    string strpayrollid = GetNodeValue(drRow, "payrollid");
                    string servicestatus = "1";
                    string employeeflag = "1";
                    string strhotelcode = GetNodeValue(drRow, "hotelcode");

                    sqlComm.CommandText = "insert employee1(serialnumber,empcode,c_name,e_name,gender,ppissuecountry," + System.Environment.NewLine +
                        "id,marital,hiretype,nextreviewdate,email," + System.Environment.NewLine +
                        "pager,businessphone,mobile,raddress,homephone," + System.Environment.NewLine +
                        "rzipcode,origin,address1,dangan,e_sur_name," + System.Environment.NewLine +
                        "e_first_name,huko,bloodtype,hrstatus,birthday," + System.Environment.NewLine +
                        "Issuecountry,ppnumber,ppexpiredate,address2," + System.Environment.NewLine +
                        "hiredate,hirevalid,hrid,payrollid,servicestatus,employeeflag) values(" + System.Environment.NewLine +
                         QuoNStr(strSerialNumber) + "," + QuoNStr(strEmpcode) + "," + QuoNStr(strCname) + "," + QuoNStr(strEname) + "," + QuoNStr(strgender) + "," + QuoNStr(strppissuecountry) + "," + System.Environment.NewLine +
                        QuoNStr(strid) + "," + QuoNStr(strmarital) + "," + QuoNStr(strhiretype) + "," + QuoNStr(strextreviewdate) + "," + QuoNStr(stremail) + "," + System.Environment.NewLine +
                        QuoNStr(strpager) + "," + QuoNStr(strbusinessphone) + "," + QuoNStr(strmobile) + "," + QuoNStr(strraddress) + "," + QuoNStr(strhomephone) + "," + System.Environment.NewLine +
                        QuoNStr(strrzipcode) + "," + QuoNStr(strorigin) + "," + QuoNStr(straddress1) + "," + QuoNStr(strdangan) + "," + QuoNStr(stre_sur_name) + "," + System.Environment.NewLine +
                        QuoNStr(stre_first_name) + "," + QuoNStr(strhuko) + "," + QuoNStr(strbloodtype) + "," + QuoNStr(strhrstatus )+ "," + QuoNStr(strbirthday) + "," + System.Environment.NewLine +
                        QuoNStr(strIssuecountry) + "," + QuoNStr(strppnumber) + "," + QuoNStr(strppexpiredate) + "," + QuoNStr(straddress2) + "," + System.Environment.NewLine +
                        QuoNStr(strhiredate) + "," + QuoNStr(strhiredate) + "," + QuoNStr(strhrid) + "," + QuoNStr(strpayrollid) + "," + QuoNStr(servicestatus) + "," + QuoNStr(employeeflag) + ")";

                    iResult = sqlComm.ExecuteNonQuery();

                    string strstructureid = GetNodeInt(drRow, "structureid");                                  

                    if (strstructureid != string.Empty)
                    {
                        sqlComm.CommandText = "update employee1 set orgstartdate=" + QuoNStr(strhiredate) +
                        " ,orgcode1=isnull((select orgcode1  from organizationstructure where structurecode =" + QuoNStr(strstructureid) + "),''), " +
                        " orgcode2=isnull((select orgcode2  from organizationstructure where structurecode =" + QuoNStr(strstructureid) + "),''), " +
                        " orgcode3=isnull((select orgcode3  from organizationstructure where structurecode =" + QuoNStr(strstructureid) + "),''), " +
                        " orgcode4=isnull((select orgcode4  from organizationstructure where structurecode =" + QuoNStr(strstructureid) + "),''), " +
                        " orgcode5=isnull((select orgcode5  from organizationstructure where structurecode =" + QuoNStr(strstructureid) + "),''), " +
                        " orgcode6=isnull((select orgcode6  from organizationstructure where structurecode =" + QuoNStr(strstructureid) + "),'')" +
                        " from employee1 where serialnumber=" + QuoNStr(strSerialNumber);
                        iResult = sqlComm.ExecuteNonQuery();

                        sqlComm.CommandText = " insert movement(serialnumber,orgcode1,orgcode2,orgcode3,orgcode4,orgcode5,orgcode6,movedate,changeddate,typeid,inout) values(" + System.Environment.NewLine +
                        QuoNStr(strSerialNumber) + "," + "isnull((select orgcode1  from organizationstructure where structurecode =" + QuoNStr(strstructureid) + "),''), " + System.Environment.NewLine +
                       "isnull((select orgcode2  from organizationstructure where structurecode =" + QuoNStr(strstructureid) + "),''), " + System.Environment.NewLine +
                       "isnull((select orgcode3  from organizationstructure where structurecode =" + QuoNStr(strstructureid) + "),''), " +System.Environment.NewLine +
                       "isnull((select orgcode4  from organizationstructure where structurecode =" + QuoNStr(strstructureid) + "),''), " +System.Environment.NewLine +
                       "isnull((select orgcode5  from organizationstructure where structurecode =" + QuoNStr(strstructureid) + "),''), " +System.Environment.NewLine +
                       "isnull((select orgcode6  from organizationstructure where structurecode =" + QuoNStr(strstructureid) + "),''), " +System.Environment.NewLine +
                        QuoNStr(strhiredate) + "," + QuoNStr(strhiredate) + ",'new','in')";
                        iResult = sqlComm.ExecuteNonQuery();
                    }

                    if (strhotelcode != string.Empty)
                    {
                        sqlComm.CommandText = "update employee1 set orgstartdate=" + QuoNStr(strhiredate) +
                         " ,orgcode1=isnull((select orgcode1  from organizationstructure where orgcode =" + QuoNStr(strhotelcode) + "),''), " + System.Environment.NewLine +
                         " orgcode2=isnull((select orgcode2  from organizationstructure where orgcode =" + QuoNStr(strhotelcode) + "),''), " + System.Environment.NewLine +
                         " orgcode3=isnull((select orgcode3  from organizationstructure where orgcode =" + QuoNStr(strhotelcode) + "),''), " + System.Environment.NewLine +
                         " orgcode4=isnull((select orgcode4  from organizationstructure where orgcode =" + QuoNStr(strhotelcode) + "),''), " + System.Environment.NewLine +
                         " orgcode5=isnull((select orgcode5  from organizationstructure where orgcode =" + QuoNStr(strhotelcode) + "),''), " + System.Environment.NewLine +
                         " orgcode6=isnull((select orgcode6  from organizationstructure where orgcode =" + QuoNStr(strhotelcode) + "),'')" + System.Environment.NewLine +
                         " from employee1 where serialnumber=" + QuoNStr(strSerialNumber);
                        iResult = sqlComm.ExecuteNonQuery();

                        sqlComm.CommandText = " insert movement(serialnumber,orgcode1,orgcode2,orgcode3,orgcode4,orgcode5,orgcode6,movedate,changeddate,typeid,inout) values(" + System.Environment.NewLine +
                        QuoNStr(strSerialNumber) + "," + "isnull((select orgcode1  from organizationstructure where structurecode =" + QuoNStr(strstructureid) + "),''), " + System.Environment.NewLine +
                       "isnull((select orgcode2  from organizationstructure where orgcode =" + QuoNStr(strhotelcode) + "),''), " + System.Environment.NewLine +
                       "isnull((select orgcode3  from organizationstructure where orgcode =" + QuoNStr(strhotelcode) + "),''), " + System.Environment.NewLine +
                       "isnull((select orgcode4  from organizationstructure where orgcode =" + QuoNStr(strhotelcode) + "),''), " + System.Environment.NewLine +
                       "isnull((select orgcode5  from organizationstructure where orgcode =" + QuoNStr(strhotelcode) + "),''), " + System.Environment.NewLine +
                       "isnull((select orgcode6  from organizationstructure where orgcode =" + QuoNStr(strhotelcode) + "),''), " + System.Environment.NewLine +
                        QuoNStr(strhiredate) + "," + QuoNStr(strhiredate) + ",'new','in')";
                        iResult = sqlComm.ExecuteNonQuery();
                    }

                    #endregion
                    //employee2,employee3
                    #region
                    sqlComm.CommandText = "insert employee2(serialnumber) values('" + strSerialNumber + "')";
                    iResult = sqlComm.ExecuteNonQuery();

                    string strhafid = GetNodeValue(drRow, "hafid");
                    string strpensionsequence = GetNodeValue(drRow, "pensionsequence");
                    string strccclass = GetNodeValue(drRow, "ccclass");
                    string strhiresociety = GetNodeValue(drRow, "hiresociety");
                    string strbase = GetNodeNumber(drRow, "base").ToString();
                    string strbankid1 = GetNodeValue(drRow, "bankaccountnumber", 30);
                    string strbankname1 = GetNodeValue(drRow, "bankfullname", 100);

                    sqlComm.CommandText = "insert employee3(serialnumber,hafid,pensionsequence,ccclass,hiresociety,base,baserate,status,bankid1,bankname1) values(" +
                        QuoNStr(strSerialNumber) + "," + QuoNStr(strhafid) + "," + QuoNStr(strpensionsequence) + "," + QuoNStr(strccclass) + "," +
                       QuoNStr(strhiresociety) + "," + QuoNStr(strbase) + "," + QuoNStr(strbase) + ",'01'," + QuoNStr(strbankid1) + "," + 
                       QuoNStr(strbankname1) + ")";
                    iResult = sqlComm.ExecuteNonQuery();
                    #endregion
                    //employee4
                    #region
                    string stroriglocation = GetNodeInt(drRow, "origlocation");
                    string strworklocation = GetNodeInt(drRow, "worklocation");
                    string strsilocation = GetNodeInt(drRow, "silocation");
                    string strtaxlocation = GetNodeInt(drRow, "taxlocation");
                    string streffectivedate = GetNodeDate(drRow, "effectivedate");

                    string strnextchange = GetNodeValue(drRow, "nextchange",50);
                    string strreason = GetNodeValue(drRow, "reason");
                    string strtotalscore = GetNodeInt(drRow, "totalscore");
                    string strevaluationindex = GetNodeInt(drRow, "evaluationindex");
                    string strsenioritiesbefore = GetNodeInt(drRow, "senioritiesbefore");

                    string strorigempno = GetNodeValue(drRow, "origempno",50);
                    string stroutsourceid = GetNodeValue(drRow, "outsourceid",30);
                    string strexpectedreportingtime = GetNodeDate(drRow, "expectedreportingtime");
                    
                    string strentryproperty = GetNodeInt(drRow, "entryproperty");

                    string strattendancetype = GetNodeInt(drRow, "attendancetype");
                    string strprobationlength = GetNodeInt(drRow, "probationlength");
                    string strisehrentry = GetNodeValue(drRow, "isehrentry");
                    string strissendedpositive = GetNodeValue(drRow, "issendedpositive");
                    string strworkstructureid = GetNodeValue(drRow, "workstructureid");

                    string strinterntype = GetNodeValue(drRow, "interntype");
                    string strlearningminister = GetNodeValue(drRow, "learningminister");
                    string strhousingchargehotel = GetNodeValue(drRow, "housingchargehotel");
                    string strsocialchargehotel = GetNodeValue(drRow, "socialchargehotel");
                    string strnccompany = GetNodeValue(drRow, "nccompany");

                    string strweddingDate = GetNodeDate(drRow, "weddingDate");
                    string strhighestdegree = GetNodeValue(drRow, "highestdegree");
                    string strhighesttitle = GetNodeValue(drRow, "highesttitle");
                    string strcurrentcity = GetNodeValue(drRow, "currentcity");


                    string strhouseholdregistertype = GetNodeValue(drRow, "householdregistertype");
                    string strpassportduetime = GetNodeDate(drRow, "passportduetime");
                    string strpassportissuetime = GetNodeDate(drRow, "passportissuetime");
                    string strpassportremark = GetNodeValue(drRow, "passportremark");
                    string strworkvisaduetime = GetNodeDate(drRow, "workvisaduetime");


                    string strresidentpermitduedate = GetNodeDate(drRow, "residentpermitduedate");
                    string strresidentpermitissueplace = GetNodeValue(drRow, "residentpermitissueplace");
                    string stremploymentduedate = GetNodeDate(drRow, "employmentduedate");
                    string stremploymentpermitissueplace = GetNodeValue(drRow, "employmentpermitissueplace");
                    string strupdateannualsalarydate = GetNodeDate(drRow, "updateannualsalarydate");

                    string strisunion = GetNodeValue(drRow, "isunion");
                    string strisabroad = GetNodeValue(drRow, "isabroad");
                    string strinitialannualleaveplan = GetNodeValue(drRow, "initialannualleaveplan");
                    string strinitialsickleaveplan = GetNodeValue(drRow, "initialsickleaveplan");
                    string stradaccount = GetNodeValue(drRow, "adaccount");

                    string strncaccount = GetNodeValue(drRow, "ncaccount");
                    string strpmsaccount = GetNodeValue(drRow, "pmsaccount");
                    string stradinitialpw = GetNodeValue(drRow, "adinitialpw");
                    string strquitremark = GetNodeValue(drRow, "quitremark");
                    string strbanktype = GetNodeValue(drRow, "banktype");
                    string strbank = GetNodeValue(drRow, "bank");
                    string strbankcurrency = GetNodeValue(drRow, "bankcurrency");
                    string strbankaccountname = GetNodeValue(drRow, "bankaccountname");

                    sqlComm.CommandText = "insert employee4(serialnumber,origlocation,worklocation,silocation,taxlocation,effectivedate," + System.Environment.NewLine +
                                    "nextchange,reason,totalscore,evaluationindex,senioritiesbefore," + System.Environment.NewLine +
                                    "origempno,outsourceid,expectedreportingtime,structureid,entryproperty," + System.Environment.NewLine +
                                    "attendancetype,probationlength,isehrentry,issendedpositive,workstructureid," + System.Environment.NewLine +

                                    "interntype,learningminister,housingchargehotel,socialchargehotel,nccompany," + System.Environment.NewLine +
                                    "weddingDate,highestdegree,highesttitle,currentcity," + System.Environment.NewLine +
                                    "householdregistertype,passportduetime,passportissuetime,passportremark,workvisaduetime," + System.Environment.NewLine +
                                    "residentpermitduedate,residentpermitissueplace,employmentduedate,employmentpermitissueplace,updateannualsalarydate," + System.Environment.NewLine +
                                    "isunion,isabroad,initialannualleaveplan,initialsickleaveplan,adaccount," + System.Environment.NewLine +
        "ncaccount,pmsaccount,adinitialpw,quitremark,banktype,bank,bankcurrency,bankaccountname) values(" + System.Environment.NewLine +
QuoNStr(strSerialNumber) + "," + QuoNStr(stroriglocation) + "," + QuoNStr(strworklocation) + "," + QuoNStr(strsilocation) + "," +QuoNStr( strtaxlocation) + "," + QuoNStr(streffectivedate) + "," + System.Environment.NewLine +
QuoNStr(strnextchange) + "," + QuoNStr(strreason) + "," + QuoNStr(strtotalscore) + "," + QuoNStr(strevaluationindex) + "," + QuoNStr(strsenioritiesbefore) + "," + System.Environment.NewLine +
QuoNStr(strorigempno) + "," + QuoNStr(stroutsourceid) + "," + QuoNStr(strexpectedreportingtime) + "," + QuoNStr(strstructureid) + "," +QuoNStr( strentryproperty) + "," + System.Environment.NewLine +
QuoNStr(strattendancetype) + "," + QuoNStr(strprobationlength) + "," +QuoNStr( strisehrentry) + "," + QuoNStr(strissendedpositive) + "," +QuoNStr( strworkstructureid) + "," + System.Environment.NewLine +

QuoNStr(strinterntype) + "," + QuoNStr(strlearningminister) + "," + QuoNStr(strhousingchargehotel) + "," + QuoNStr(strsocialchargehotel) + "," + QuoNStr(strnccompany) + "," + System.Environment.NewLine +
QuoNStr(strweddingDate) + "," + QuoNStr(strhighestdegree) + "," + QuoNStr(strhighesttitle) + "," + QuoNStr(strcurrentcity) + "," + System.Environment.NewLine +
QuoNStr( strhouseholdregistertype) + "," + QuoNStr(strpassportduetime) + "," +QuoNStr( strpassportissuetime) + "," +QuoNStr( strpassportremark) + "," +QuoNStr( strworkvisaduetime) + "," + System.Environment.NewLine +
QuoNStr(strresidentpermitduedate) + "," + QuoNStr(strresidentpermitissueplace) + "," + QuoNStr(stremploymentduedate) + "," + QuoNStr(stremploymentpermitissueplace) + "," + QuoNStr(strupdateannualsalarydate) + "," + System.Environment.NewLine +
QuoNStr( strisunion) + "," + QuoNStr(strisabroad) + "," + QuoNStr( strinitialannualleaveplan) + "," + QuoNStr(strinitialsickleaveplan) + "," +QuoNStr( stradaccount) + "," + System.Environment.NewLine +
QuoNStr(strncaccount) + "," + QuoNStr(strpmsaccount) + "," + QuoNStr(stradinitialpw) + "," + QuoNStr(strquitremark) + "," +
QuoNStr(strbanktype) + "," + QuoNStr(strbank) + "," + QuoNStr(strbankcurrency) + "," + QuoNStr(strbankaccountname) + ")";

                    iResult = sqlComm.ExecuteNonQuery();
                    #endregion

                    //empposition
                    #region
                    string strpositioncode = GetNodeValue(drRow, "positioncode");
                    string strgrade = GetNodeValue(drRow, "grade");
                    string strpositionstartdate = GetNodeValue(drRow, "positionstartdate");
                    sqlComm.CommandText = "insert empposition(serialnumber,positioncode,currentposition,grade,positionstartdate,changereason) values(" +
                    QuoNStr(strSerialNumber) + "," + QuoNStr(strpositioncode) + ",1," + QuoNStr(strgrade) + "," +QuoNStr( strhiredate) + ",'new')";
                    iResult = sqlComm.ExecuteNonQuery();
                    #endregion

                    //employee_system
                    #region
                    sqlComm.CommandText = " insert employee_system(serialnumber,systemid,effectivedate,currentflag,notes) values(" +
                    QuoNStr(strSerialNumber) + "," + QuoNStr(strpayrollid) + "," + QuoNStr(strhiredate) + ",1,'Huazhu OA')";
                    iResult = sqlComm.ExecuteNonQuery();
                    #endregion
                   

                    if (iResult >= 1)
                    {
                        retXML = GetResultTable(retXML, true, strEmpcode, "Adding is OK", true);
                        sqlTran.Commit();
                    }
                    else
                    {
                        retXML = GetResultTable(retXML, false, strEmpcode, "Adding is failure", true);
                        sqlTran.Rollback();
                    }
                    
                }
                                
                return retXML;
            }
            catch (XmlException ex)
            {
                
                retXML = GetResultTable(retXML, false, "All", "Adding is failure", true);
                return retXML;
            }
        }
        
        //--酒店异动
        //IsHotelChange                           是否部门变动
        //IsPositionChange                是否职位变动
        //IsCostCenterChange           是否成本中心变动
        //IsPositionLevel                    是否职级变动
        //IsBasicSalaryChange           是否基本工资变动
        //IsWorkLocationChange             是否工作地变动
        //JobChangeEffectiveDate     异动日期
        //SalaryEffectiveDate            薪资变动日期
        //OutSource                                外包类型
        //SocialChargeHotel                     社保代缴酒店
        //Reason                                      异动原因
  
        public DataTable ChangeEmployeeHotel(DataTable dtTable)
        {
            string strSql = string.Empty;
            SqlDataAdapter sqlData = new SqlDataAdapter();
            DataSet dsData = new DataSet();
            SqlTransaction sqlTran;
            sqlTran = sqlConn.BeginTransaction();
            sqlComm.Transaction = sqlTran;
            int iResult = 0;
            DataTable retXML = new DataTable("Results");
            string strSerialNumber = "";

            try
            {
                if (dtTable == null || dtTable.Rows.Count <= 0)
                    return null;

                foreach (DataRow drRow in dtTable.Rows)
                {
                    string strEmpcode = GetNodeValue(drRow, "empcode", 20);

                    sqlComm.CommandText = "select serialnumber from employee1 where empcode='" + strEmpcode + "'";
                    sqlData.SelectCommand = sqlComm;
                    dsData= new DataSet();
                    sqlData.Fill(dsData);

                    if (dsData == null || dsData.Tables.Count <= 0 || dsData.Tables[0].Rows.Count <= 0)
                    {
                        retXML = GetResultTable(retXML, false, strEmpcode, "Not exist", true);
                        continue;
                    }
                    strSerialNumber = dsData.Tables[0].Rows[0][0].ToString();
                    string strChange = GetNodeValue(drRow, "changeid");          
                    bool IsHotelChange = bool.Parse(GetNodeBool(drRow, "IsHotelChange"));                        
                    bool IsPositionChange =bool.Parse(GetNodeBool(drRow, "IsPositionChange"));    
                    bool IsCostCenterChange=bool.Parse(GetNodeBool(drRow, "IsCostCenterChange"));
                    bool IsPositionLevel = bool.Parse(GetNodeValue(drRow, "IsPositionLevel"));
                    bool IsBasicSalaryChange = bool.Parse(GetNodeValue(drRow, "IsBasicSalaryChange"));
                    bool IsWorkLocationChange = bool.Parse(GetNodeValue(drRow, "IsWorkLocationChange"));       
                    string JobChangeEffectiveDate =GetNodeDate(drRow, "JobChangeEffectiveDate");    
                    string SalaryEffectiveDate =GetNodeDate(drRow, "SalaryEffectiveDate");
                    string OutSource = GetNodeValue(drRow, "OutSource");                  
                    string SocialChargeHotel=GetNodeValue(drRow, "SocialChargeHotel");                    
                    string Reason =GetNodeValue(drRow, "Reason"); 
                    string strbase =GetNodeNumber(drRow,"base").ToString(); 
                    string strpositioncode = GetNodeValue(drRow, "positioncode",10);
                    string strorgcode = GetNodeValue(drRow, "orgcode");
                    string strpositionlevel = GetNodeValue(drRow, "positionlevel",10);
                    string strccclass = GetNodeValue(drRow, "costcenter",10);
                    string strhotelcode = GetNodeValue(drRow, "HotelCode", 10);
               
                    //salarychange
                    #region
                    if (IsBasicSalaryChange)
                    {
                        sqlComm.CommandText = "insert salaryhistory(serialnumber,payrollid,paychangedate,paystartdate,payenddate,base,baserate)" +
                            " select serialnumber,payrollid,paychangedate,paystartdate," +
                            QuoNStr(DateTime.Parse(SalaryEffectiveDate).AddDays(-1).ToShortDateString()) + ",base,baserate from employee3" +
                            " where serialnumber=" + QuoNStr(strSerialNumber);
                        iResult = sqlComm.ExecuteNonQuery();

                        sqlComm.CommandText = "update employee3 set base=" + strbase + ",baserate=" + QuoNStr(strbase) +
                            ",paystartdate=" + QuoNStr(SalaryEffectiveDate) + ",paychangedate=" + QuoNStr(SalaryEffectiveDate) +
                            " where serialnumber=" + QuoNStr(strSerialNumber);
                        iResult = sqlComm.ExecuteNonQuery();
                    }
                    #endregion
                    //positionchange
                    #region
                    if (IsPositionChange)
                    {
                        sqlComm.CommandText = "update empposition set currentposition=0 where serialnumber='" + strSerialNumber + "'";
                        iResult = sqlComm.ExecuteNonQuery();

                        sqlComm.CommandText = "insert empposition(serialnumber,positioncode,positionstartdate,currentposition) " +
                            "values(" + QuoNStr(strSerialNumber) + "," + QuoNStr(strpositioncode) + "," + QuoNStr(JobChangeEffectiveDate) + ",1)";
                        iResult = sqlComm.ExecuteNonQuery();

                        sqlComm.CommandText = "update empposition set c_title=c_name ,e_title=e_name " +
                                "from empposition inner join position on empposition.positioncode=position.positioncode " +
                                "where currentposition =1 and serialnumber=" + QuoNStr(strSerialNumber);
                        iResult = sqlComm.ExecuteNonQuery();
                    }
                    #endregion
                    //organizationchange
                    #region
                    if (IsHotelChange)
                    {
                        sqlComm.CommandText = " insert movement(serialnumber,orgcode1,orgcode2,orgcode3,orgcode4,orgcode5,orgcode6,movedate,changeddate,typeid,inout) " +
                        " select serialnumber,orgcode1,orgcode2,orgcode3,orgcode4,orgcode5,orgcode6,orgstartdate," + QuoNStr(JobChangeEffectiveDate) + ",'transfer','out' from employee1 " +
                        " where serialnumber=" + QuoNStr(strSerialNumber);
                        iResult = sqlComm.ExecuteNonQuery();

                        sqlComm.CommandText = "insert movement(serialnumber,orgcode1,orgcode2,orgcode3,orgcode4,orgcode5,orgcode6,movedate,changeddate,typeid,inout) " +
                                " select " + QuoNStr(strSerialNumber) + ",orgcode1,orgcode2,orgcode3,orgcode4,orgcode5,orgcode6," +
                                QuoNStr(JobChangeEffectiveDate) + "," + QuoNStr(JobChangeEffectiveDate) +
                                " ,'transfer','in' from organizationstructure where orgcode=" + QuoNStr(strhotelcode);
                        iResult = sqlComm.ExecuteNonQuery();

                        sqlComm.CommandText = "update employee1 set orgstartdate=" + QuoNStr(JobChangeEffectiveDate) +
                                " ,orgcode1=isnull((select orgcode1  from organizationstructure where orgcode =" + QuoNStr(strhotelcode) + "),''), " +
                                " orgcode2=isnull((select orgcode2  from organizationstructure where orgcode =" + QuoNStr(strhotelcode) + "),''), " +
                                " orgcode3=isnull((select orgcode3  from organizationstructure where orgcode =" + QuoNStr(strhotelcode) + "),''), " +
                                " orgcode4=isnull((select orgcode4  from organizationstructure where orgcode =" + QuoNStr(strhotelcode) + "),''), " +
                                " orgcode5=isnull((select orgcode5  from organizationstructure where orgcode =" + QuoNStr(strhotelcode) + "),''), " +
                                " orgcode6=" + QuoNStr(strhotelcode) +
                                " from employee1 where serialnumber=" + QuoNStr(strSerialNumber);
                        iResult = sqlComm.ExecuteNonQuery();
                    }
                    #endregion
                    //levelchange
                    #region
                    if (IsPositionLevel)
                    {
                        sqlComm.CommandText = "insert fieldaudit(serialnumber,tablename,fieldaudit,fieldname,olddata,newdata,validdate,mod_time,mod_user)" +
                                " select " + QuoNStr(strSerialNumber) + ",'employee1',1,'grade',grade," + QuoNStr(strpositionlevel) +
                                "," + QuoNStr(JobChangeEffectiveDate) + ",getdate() ,'HuazhuOA' from employee1 where serialnumber=" + QuoNStr(strSerialNumber);
                        iResult = sqlComm.ExecuteNonQuery();

                        sqlComm.CommandText = "update employee1 set grade=" + QuoNStr(strpositionlevel) + " where serialnumber=" + QuoNStr(strSerialNumber);
                        iResult = sqlComm.ExecuteNonQuery();
                    }
                    #endregion
                    //costchange
                    #region
                    if (IsCostCenterChange)
                    {
                        sqlComm.CommandText = "insert fieldaudit(serialnumber,systemid,tablename,fieldaudit,fieldname,olddata,newdata,validdate,mod_time,mod_user)" +
                                " select " + QuoNStr(strSerialNumber) + ",payrollid,'employee3',1,'ccclass',ccclass," + QuoNStr(strccclass) +
                                "," + QuoNStr(JobChangeEffectiveDate) + ",getdate(),'HuazhuOA' from employee3 where serialnumber=" + QuoNStr(strSerialNumber);
                        iResult = sqlComm.ExecuteNonQuery();

                        sqlComm.CommandText = "update employee3 set ccclass=" + QuoNStr(strccclass) + " where serialnumber=" + QuoNStr(strSerialNumber);
                        iResult = sqlComm.ExecuteNonQuery();
                    }

                    if (OutSource != string.Empty)
                    {
                        sqlComm.CommandText = "update employee4 set outsourceid=" + QuoNStr(OutSource) + " where serialnumber=" + QuoNStr(strSerialNumber);
                        iResult = sqlComm.ExecuteNonQuery();

                        sqlComm.CommandText = "insert fieldaudit(serialnumber,tablename,fieldaudit,fieldname,olddata,newdata,validdate,mod_time,mod_user)" +
                         " select " + QuoNStr(strSerialNumber) + ",'employee4',1,'outsourceid',outsourceid," + QuoNStr(OutSource) +
                         "," + QuoNStr(JobChangeEffectiveDate) + ",getdate() ,'HuazhuOA' from employee4 where serialnumber=" + QuoNStr(strSerialNumber);
                                    iResult = sqlComm.ExecuteNonQuery();
                    }

                    if (SocialChargeHotel != string.Empty)
                    {
                        sqlComm.CommandText = "update employee4 set socialchargehotel=" + QuoNStr(SocialChargeHotel) + " where serialnumber=" + QuoNStr(strSerialNumber);
                        iResult = sqlComm.ExecuteNonQuery();

                        sqlComm.CommandText = "insert fieldaudit(serialnumber,tablename,fieldaudit,fieldname,olddata,newdata,validdate,mod_time,mod_user)" +
                         " select " + QuoNStr(strSerialNumber) + ",'employee4',1,'socialchargehotel',socialchargehotel," + QuoNStr(SocialChargeHotel) +
                         "," + QuoNStr(JobChangeEffectiveDate) + ",getdate() ,'HuazhuOA' from employee4 where serialnumber=" + QuoNStr(strSerialNumber);
                                                iResult = sqlComm.ExecuteNonQuery();
                    }
                        
                    #endregion

                      if (iResult >= 1)
                      {
                          retXML = GetResultTable(retXML, true, strEmpcode,strChange, "Changing is OK", true);
                      }
                      else
                      {
                          retXML = GetResultTable(retXML, false, strEmpcode,strChange, "Changing is failure", true);
                      }

                }

                sqlTran.Commit();
                return retXML;
            }
            catch (XmlException ex)
            {
                sqlTran.Rollback();
                Console.Write(ex.Message);
                return null;
            }
        }
        
        //                --总部分公司异动
        //IsDepartChange                        是否部门变动
        //IsSuperiorChange               是否上级变动
        //IsPositionChange                是否职位变动
        //IsCostCenterChange           是否成本中心变动
        //IsPositionLevel                    是否职级变动
        //IsBasicSalaryChange           是否基本工资变动
        //IsWorkLocationChange             是否工作地变动
        //JobChangeEffectiveDate     异动日期
        //SalaryEffectiveDate            薪资变动日期
        //OutSource                                外包类型
        //NCCompany                             NC公司编码
        //SocialChargeHotel                     社保代缴酒店
        //HousingChargeHotel          公积金代缴酒店
        //Reason                                      异动原因
        public DataTable ChangeEmployeeOffice(DataTable dtTable)
        {
            string strSql = string.Empty;
            SqlDataAdapter sqlData = new SqlDataAdapter();
            DataSet dsData = new DataSet();
            SqlTransaction sqlTran;
            sqlTran = sqlConn.BeginTransaction();
            sqlComm.Transaction = sqlTran;
            int iResult = 0;
            DataTable retXML = new DataTable("Results");
            string strSerialNumber = "";

            try
            {
                if (dtTable == null || dtTable.Rows.Count <= 0)
                    return null;

                foreach (DataRow drRow in dtTable.Rows)
                {
                    string strEmpcode = GetNodeValue(drRow, "empcode", 20);

                    sqlComm.CommandText = "select serialnumber from employee1 where empcode='" + strEmpcode + "'";
                    sqlData.SelectCommand = sqlComm;
                    dsData= new DataSet();
                    sqlData.Fill(dsData);

                    if (dsData == null || dsData.Tables.Count <= 0 || dsData.Tables[0].Rows.Count <= 0)
                    {
                        retXML = GetResultTable(retXML, false, strEmpcode, "Not exist", true);
                        continue;
                    }

                    strSerialNumber = dsData.Tables[0].Rows[0][0].ToString();
                    string strChange = GetNodeValue(drRow, "changeid");       
                    bool IsSuperiorChange = bool.Parse(GetNodeBool(drRow, "IsSuperiorChange"));
                    bool IsDepartChange = bool.Parse(GetNodeBool(drRow, "IsDepartChange"));                        
                    bool IsPositionChange =bool.Parse(GetNodeBool(drRow, "IsPositionChange"));    
                    bool IsCostCenterChange=bool.Parse(GetNodeBool(drRow, "IsCostCenterChange"));
                    bool IsPositionLevel = bool.Parse(GetNodeValue(drRow, "IsPositionLevel"));
                    bool IsBasicSalaryChange = bool.Parse(GetNodeValue(drRow, "IsBasicSalaryChange"));
                    bool IsWorkLocationChange = bool.Parse(GetNodeValue(drRow, "IsWorkLocationChange"));    
                    string JobChangeEffectiveDate =GetNodeDate(drRow, "JobChangeEffectiveDate");    
                    string SalaryEffectiveDate =GetNodeDate(drRow, "SalaryEffectiveDate");
                    string OutSource = GetNodeValue(drRow, "OutSource");                  
                    string SocialChargeHotel=GetNodeValue(drRow, "SocialChargeHotel");
                    string HousingChargeHotel = GetNodeValue(drRow, "HousingChargeHotel");
                    string NCCompany = GetNodeValue(drRow, "NCCompany"); 
                    string Reason =GetNodeValue(drRow, "Reason");

                    string strbase = GetNodeNumber(drRow, "base").ToString(); 
                    string strpositioncode = GetNodeValue(drRow, "positioncode",10);
                    string strorgcode = GetNodeValue(drRow, "orgcode");
                    string strpositionlevel = GetNodeValue(drRow, "positionlevel",10);
                    string strccclass = GetNodeValue(drRow, "costcenter",10);
                    //salarychange
                    #region
                    if (IsBasicSalaryChange)
                    {
                        sqlComm.CommandText = "insert salaryhistory(serialnumber,payrollid,paychangedate,paystartdate,payenddate,base,baserate)" +
                            " select serialnumber,payrollid,paychangedate,paystartdate," +
                            QuoNStr(DateTime.Parse(SalaryEffectiveDate).AddDays(-1).ToShortDateString()) + ",base,baserate from employee3" +
                            " where serialnumber=" + QuoNStr(strSerialNumber);
                        iResult = sqlComm.ExecuteNonQuery();

                        sqlComm.CommandText = "update employee3 set base=" + strbase + ",baserate=" + QuoNStr(strbase) +
                            ",paystartdate=" + QuoNStr(SalaryEffectiveDate) + ",paychangedate=" + QuoNStr(SalaryEffectiveDate) +
                            " where serialnumber=" + QuoNStr(strSerialNumber);
                        iResult = sqlComm.ExecuteNonQuery();
                    }
                    #endregion
                    //positionchange
                    #region
                    if (IsPositionChange)
                    {
                        sqlComm.CommandText = "update empposition set currentposition=0 where serialnumber='" + strSerialNumber + "'";
                        iResult = sqlComm.ExecuteNonQuery();

                        sqlComm.CommandText = "insert empposition(serialnumber,positioncode,positionstartdate,currentposition) " +
                            "values(" + QuoNStr(strSerialNumber) + "," + QuoNStr(strpositioncode) + "," + QuoNStr(JobChangeEffectiveDate) + ",1)";
                        iResult = sqlComm.ExecuteNonQuery();

                        sqlComm.CommandText = "update empposition set c_title=c_name ,e_title=e_name " +
                                "from empposition inner join position on empposition.positioncode=position.positioncode " +
                                "where currentposition =1 and serialnumber=" + QuoNStr(strSerialNumber);
                        iResult = sqlComm.ExecuteNonQuery();
                    }
                    #endregion
                    //organizationchange
                    #region
                    if (IsDepartChange)
                    {
                        sqlComm.CommandText = " insert movement(serialnumber,orgcode1,orgcode2,orgcode3,orgcode4,orgcode5,orgcode6,movedate,changeddate,typeid,inout) " +
                        " select serialnumber,orgcode1,orgcode2,orgcode3,orgcode4,orgcode5,orgcode6,orgstartdate," + QuoNStr(JobChangeEffectiveDate) + ",'transfer','out' from employee1 " +
                        " where serialnumber=" + QuoNStr(strSerialNumber);
                        iResult = sqlComm.ExecuteNonQuery();

                        sqlComm.CommandText = "insert movement(serialnumber,orgcode1,orgcode2,orgcode3,orgcode4,orgcode5,orgcode6,movedate,changeddate,typeid,inout) " +
                                " select " + QuoNStr(strSerialNumber) + ",orgcode1,orgcode2,orgcode3,orgcode4,orgcode5,orgcode6," +
                                QuoNStr(JobChangeEffectiveDate) + "," + QuoNStr(JobChangeEffectiveDate) +
                                " ,'transfer','in' from organizationstructure where structurecode=" + QuoNStr(strorgcode);
                        iResult = sqlComm.ExecuteNonQuery();

                        sqlComm.CommandText = "update employee1 set orgstartdate=" + QuoNStr(JobChangeEffectiveDate) +
                                " ,orgcode1=isnull((select orgcode1  from organizationstructure where structurecode =" + QuoNStr(strorgcode) + "),''), " +
                                " orgcode2=isnull((select orgcode2  from organizationstructure where structurecode =" + QuoNStr(strorgcode) + "),''), " +
                                " orgcode3=isnull((select orgcode3  from organizationstructure where structurecode =" + QuoNStr(strorgcode) + "),''), " +
                                " orgcode4=isnull((select orgcode4  from organizationstructure where structurecode =" + QuoNStr(strorgcode) + "),''), " +
                                " orgcode5=isnull((select orgcode5  from organizationstructure where structurecode =" + QuoNStr(strorgcode) + "),''), " +
                                " orgcode6=isnull((select orgcode6  from organizationstructure where structurecode =" + QuoNStr(strorgcode) + "),'')" +
                                " from employee1 where serialnumber=" + QuoNStr(strSerialNumber);
                        iResult = sqlComm.ExecuteNonQuery();
                    }
                    #endregion
                    //levelchange
                    #region
                    if (IsPositionLevel)
                    {
                        sqlComm.CommandText = "insert fieldaudit(serialnumber,tablename,fieldaudit,fieldname,olddata,newdata,validdate,mod_time,mod_user)" +
                                " select " + QuoNStr(strSerialNumber) + ",'employee1',1,'grade',grade," + QuoNStr(strpositionlevel) +
                                "," + QuoNStr(JobChangeEffectiveDate) + ",getdate() ,'HuazhuOA' from employee1 where serialnumber=" + QuoNStr(strSerialNumber);
                        iResult = sqlComm.ExecuteNonQuery();

                        sqlComm.CommandText = "update employee1 set grade=" + QuoNStr(strpositionlevel) + " where serialnumber=" + QuoNStr(strSerialNumber);
                        iResult = sqlComm.ExecuteNonQuery();
                    }
                    #endregion
                    //costchange
                    #region
                    if (IsCostCenterChange)
                    {
                        sqlComm.CommandText = "insert fieldaudit(serialnumber,systemid,tablename,fieldaudit,fieldname,olddata,newdata,validdate,mod_time,mod_user)" +
                                " select " + QuoNStr(strSerialNumber) + ",payrollid,'employee3',1,'ccclass',ccclass," + QuoNStr(strccclass) +
                                "," + QuoNStr(JobChangeEffectiveDate) + ",getdate(),'HuazhuOA' from employee3 where serialnumber=" + QuoNStr(strSerialNumber);
                        iResult = sqlComm.ExecuteNonQuery();

                        sqlComm.CommandText = "update employee3 set ccclass=" + QuoNStr(strccclass) + " where serialnumber=" + QuoNStr(strSerialNumber);
                        iResult = sqlComm.ExecuteNonQuery();
                    }
                    #endregion

                    if (iResult >= 1)
                    {
                        retXML = GetResultTable(retXML, true, strEmpcode, strChange,"Changing is OK", true);
                    }
                    else
                    {
                        retXML = GetResultTable(retXML, false, strEmpcode, strChange,"Changing is failure", true);
                    }

                }

                sqlTran.Commit();
                return retXML;
            }
            catch (XmlException ex)
            {
                sqlTran.Rollback();
                Console.Write(ex.Message);
                return null;
            }
}

        public DataTable SetPositionData(DataTable dtTable)
        {
            string strSql = string.Empty;
            SqlDataAdapter sqlData = new SqlDataAdapter();
            DataSet dsData = new DataSet();
            SqlTransaction sqlTran;
            sqlTran = sqlConn.BeginTransaction();
            sqlComm.Transaction = sqlTran;
            int iResult = 0;
            DataTable retXML = new DataTable("Results");

            try
            {
                sqlComm.CommandText = "  delete from position";
                iResult = sqlComm.ExecuteNonQuery();

                foreach (DataRow dtRow in dtTable.Rows)
                {
                    string strCode = GetNodeValue(dtRow, "positioncode", 10);
                    string strCName = GetNodeValue(dtRow, "c_name", 100);
                    string strEName = GetNodeValue(dtRow, "e_name", 100);
                    string strHotelValue = GetNodeValue(dtRow, "jobdescription", 100);

                    sqlComm.CommandText = "Insert position(positioncode,hrid,c_name,e_name,jobdescription) values('" +
                            strCode + "',1,N'" + strCName + "',N'" + strEName + "',N'" + strHotelValue + "')";
                    iResult = sqlComm.ExecuteNonQuery();

                    if (iResult >= 1)
                    {
                        retXML = GetResultTable(retXML, true, strCode, "Adding is OK", true);
                    }
                    else
                    {
                        retXML = GetResultTable(retXML, false, strCode, "Adding is failure", true);
                    }
                }
                sqlTran.Commit();
                return retXML;
            }
            catch (XmlException ex)
            {
                sqlTran.Rollback();
                Console.Write(ex.Message);
                return null;
            }
        }

        public DataTable SetPositionLevelData(DataTable dtTable)
        {
            string strSql = string.Empty;
            SqlDataAdapter sqlData = new SqlDataAdapter();
            DataSet dsData = new DataSet();
            SqlTransaction sqlTran;
            sqlTran = sqlConn.BeginTransaction();
            sqlComm.Transaction = sqlTran;
            int iResult = 0;
            DataTable retXML = new DataTable("Results");

            try
            {
                sqlComm.CommandText = "  delete from grade";
                iResult = sqlComm.ExecuteNonQuery();
                
                foreach (DataRow dtRow in dtTable.Rows)
                {
                    string strCode = GetNodeValue(dtRow, "code", 10);
                    string strCName = GetNodeValue(dtRow, "c_name", 100);
                    string strEName = GetNodeValue(dtRow, "e_name", 100);

                    sqlComm.CommandText = "Insert grade(code,c_name,e_name,hrid) values('" +
                            strCode + "',N'" + strCName + "',N'" + strEName + "',1)";
                    iResult = sqlComm.ExecuteNonQuery();


                    if (iResult >= 1)
                    {
                        retXML = GetResultTable(retXML, true, strCode, "Adding is OK", true);
                    }
                    else
                    {
                        retXML = GetResultTable(retXML, false, strCode, "Adding is failure", true);
                    }
                }

                sqlTran.Commit();
                return retXML;
            }
            catch (XmlException ex)
            {
                sqlTran.Rollback();
                Console.Write(ex.Message);
                return null;
            }
        }

        public DataTable SaveMonthImport(DataTable dtTable)
        {
            string strSql = string.Empty;
            SqlDataAdapter sqlData = new SqlDataAdapter();
            SqlDataAdapter sqlRow = new SqlDataAdapter();
            DataSet dsData = new DataSet();
            DataTable dtMonth;
            SqlTransaction sqlTran;
            sqlTran = sqlConn.BeginTransaction();
            sqlComm.Transaction = sqlTran;
            int iResult = 0;
            DataTable retXML = new DataTable("Results");
            DataTable dtType = new DataTable();

            try
            {
                sqlComm.CommandText = "select catecode code,'S' + LEFT(catecode,1) typeid from paycategory union " +
                    " select code,'OT' typeid from ottype union " +
                    " select code,'LV' typeid from leavetype union " +
                    " select code,'SHIFT' typeid from shifttype ";
                sqlData.SelectCommand = sqlComm;
                sqlData.Fill(dtType);

                foreach (DataRow dtRow in dtTable.Rows)
                {
                    string strEmpcode = GetNodeValue(dtRow, "empcode", 20);
                    string strCode = GetNodeValue(dtRow, "code", 10);
                    double dblAmount = GetNodeNumber(dtRow, "amount");

                    sqlComm.CommandText = "select serialnumber from employee1 where empcode='" + strEmpcode + "'";
                    sqlData.SelectCommand = sqlComm;
                    dsData = new DataSet();
                    sqlData.Fill(dsData);

                    if (dsData == null || dsData.Tables.Count <= 0 || dsData.Tables[0].Rows.Count <= 0)
                    {
                        retXML = GetResultTable(retXML, false, strEmpcode, "Not exist", strCode, dblAmount.ToString(), true);
                        continue;
                    }

                    string strSerialNumber = dsData.Tables[0].Rows[0][0].ToString();
                    DataRow[] drRows = dtType.Select("code = '" + strCode + "'");
                                  
                    
                    if (drRows.Length > 0)
                    {
                        string strTypeid = drRows[0]["typeid"].ToString();
                        sqlComm.CommandText = "select * from employeetotaldetail where serialnumber=" + QuoNStr(strSerialNumber) + " and code=" + QuoNStr(strCode);
                        sqlRow.SelectCommand = sqlComm;
                        dtMonth = new DataTable();
                        sqlRow.Fill(dtMonth);
                        if (dtMonth.Rows.Count >= 1)
                        {
                            sqlComm.CommandText = " update employeetotaldetail set amount=" + dblAmount + "  where serialnumber=" + QuoNStr(strSerialNumber) + " and code=" + QuoNStr(strCode);
                            iResult = sqlComm.ExecuteNonQuery();
                            if (iResult >= 1)
                            {
                                retXML = GetResultTable(retXML, true, strEmpcode,"Updating",strCode,dblAmount.ToString(), true);
                            }
                            else
                            {
                                retXML = GetResultTable(retXML, false, strEmpcode,"Updating",strCode,dblAmount.ToString(), true);
                            }
                        }
                        else
                        {
                            sqlComm.CommandText = " insert employeetotaldetail(serialnumber,typeid,code,amount,updatetime,usercode) values(" +
                                    QuoNStr(strSerialNumber) + "," + QuoNStr(strTypeid) + "," + QuoNStr(strCode) + "," + dblAmount + ",getdate(),'Huazhu')";
                            iResult = sqlComm.ExecuteNonQuery();
                            if (iResult >= 1)
                            {
                                retXML = GetResultTable(retXML, true, strEmpcode, "Adding", strCode, dblAmount.ToString(), true);
                            }
                            else
                            {
                                retXML = GetResultTable(retXML, false, strEmpcode, "Adding", strCode, dblAmount.ToString(),true);
                            }
                        }
                    }
                    else
                    {
                        retXML = GetResultTable(retXML, false, strEmpcode, strCode + " isn't exist", strCode, dblAmount.ToString(), true);
                        continue;
                    }

                   
                }

                sqlTran.Commit();
                return retXML;
            }
            catch (XmlException ex)
            {
                sqlTran.Rollback();
                Console.Write(ex.Message);
                return null;
            }
        }

        public DataTable SetOutsourceData(DataTable dtTable)
        {
            string strSql = string.Empty;
            SqlDataAdapter sqlData = new SqlDataAdapter();
            DataSet dsData = new DataSet();
            SqlTransaction sqlTran;
            sqlTran = sqlConn.BeginTransaction();
            sqlComm.Transaction = sqlTran;
            int iResult = 0;
            DataTable retXML = new DataTable("Results");

            try
            {
                sqlComm.CommandText = " delete from parameters where paracode='outsource'";
                iResult = sqlComm.ExecuteNonQuery();

                foreach (DataRow dtRow in dtTable.Rows)
                {
                    string strCode = GetNodeValue(dtRow, "itemcode", 10);
                    string strCName = GetNodeValue(dtRow, "c_name", 100);
                    string strEName = GetNodeValue(dtRow, "e_name", 100);

                    sqlComm.CommandText = "insert parameters(paratype,paracode,itemcode,c_name,e_name) values(0,'outsource','" +
                            strCode + "',N'" + strCName + "',N'" + strEName + "')";
                    iResult = sqlComm.ExecuteNonQuery();


                    if (iResult >= 1)
                    {
                        retXML = GetResultTable(retXML, true, strCode, "Adding is OK", true);
                    }
                    else
                    {
                        retXML = GetResultTable(retXML, false, strCode, "Adding is failure", true);
                    }
                }

                sqlTran.Commit();
                return retXML;
            }
            catch (XmlException ex)
            {
                sqlTran.Rollback();
                Console.Write(ex.Message);
                return null;
            }
        }

        public DataTable SetCostcenterData(DataTable dtTable)
        {
            string strSql = string.Empty;
            SqlDataAdapter sqlData = new SqlDataAdapter();
            DataSet dsData = new DataSet();
            SqlTransaction sqlTran;
            sqlTran = sqlConn.BeginTransaction();
            sqlComm.Transaction = sqlTran;
            int iResult = 0;
            DataTable retXML = new DataTable("Results");

            try
            {
                sqlComm.CommandText = "  delete from costcenter";
                iResult = sqlComm.ExecuteNonQuery();

                sqlComm.CommandText = "  delete from ccclass_h";
                iResult = sqlComm.ExecuteNonQuery();

                foreach (DataRow dtRow in dtTable.Rows)
                {
                    string strCode = GetNodeValue(dtRow, "code", 10);
                    string strCName = GetNodeValue(dtRow, "c_name", 100);
                    string strEName = GetNodeValue(dtRow, "e_name", 100);
                    string strCategory = GetNodeValue(dtRow, "category", 100);

                    sqlComm.CommandText = "Insert costcenter(code,c_name,e_name,category) values('" +
                            strCode + "',N'" + strCName + "',N'" + strEName + "',N'" + strCategory + "')";
                    iResult = sqlComm.ExecuteNonQuery();
                    
                    if (iResult >= 1)
                    {
                        retXML = GetResultTable(retXML, true, strCode, "Adding is OK", true);
                    }
                    else
                    {
                        retXML = GetResultTable(retXML, false, strCode, "Adding is failure", true);
                    }
                }

                sqlComm.CommandText = " insert costcenter(code,c_name,e_name,payrollid,category)" +
                     " select code,costcenter.c_name,costcenter.e_name,payroll_control.payrollid,category " +
                     " from costcenter cross join payroll_control " +
                     " where costcenter.payrollid=''";
                iResult = sqlComm.ExecuteNonQuery();

                sqlComm.CommandText = " insert ccclass_h(classcode,e_name,c_name,cccategory,costcode,payrollid,typecode)" +
                    " select code,e_name,c_name,category,code,payrollid,0 from costcenter";
                iResult = sqlComm.ExecuteNonQuery();

                sqlTran.Commit();

                return retXML;
            }
            catch (XmlException ex)
            {
                sqlTran.Rollback();
                Console.Write(ex.Message);
                return null;
            }
        }

        //public string SetOrganizationData(string strXML)
        //{
        //    string strSql = string.Empty;
        //    SqlDataAdapter sqlData = new SqlDataAdapter();
        //    DataSet dsData = new DataSet();
        //    SqlTransaction sqlTran;
        //    XmlDocument xmlDoc = new XmlDocument();
        //    sqlTran = sqlConn.BeginTransaction();
        //    sqlComm.Transaction = sqlTran;
        //    int iResult = 0;
        //    XmlDocument retXML = new XmlDocument();

        //    try
        //    {
        //        xmlDoc.LoadXml(strXML);
        //        XmlNode retNode = SetXMLNode(retXML, "Results", "");
        //        retXML.AppendChild(retNode);

        //        sqlComm.CommandText = " delete from organizationstructure";
        //        iResult = sqlComm.ExecuteNonQuery();

        //        sqlComm.CommandText = " delete from organization";
        //        iResult = sqlComm.ExecuteNonQuery();

        //        if (xmlDoc.SelectNodes("DataRows").Count <= 0)
        //            return retXML.OuterXml;

        //        #region
        //        for (int i = 0; i < xmlDoc.SelectSingleNode("DataRows").SelectNodes("DataRow").Count; i++)
        //        {
        //            XmlNode node = xmlDoc.SelectSingleNode("DataRows").SelectNodes("DataRow").Item(i);
        //            string strCode = node.SelectSingleNode("code").InnerText;
        //            if (strCode.Length > 10)
        //                strCode = strCode.Substring(0, 10);

        //            string strCName = node.SelectSingleNode("cname").InnerText;
        //            if (strCName.Length > 10)
        //                strCName = strCName.Substring(0, 100);

        //            string strEName = node.SelectSingleNode("ename").InnerText;
        //            if (strEName.Length > 100)
        //                strEName = strEName.Substring(0, 100);

        //            string strTypeid = node.SelectSingleNode("typeid").InnerText;
        //            string strparentid = node.SelectSingleNode("parentid").InnerText;

        //            sqlComm.CommandText = "insert organization(code,typeid,hrid,c_name,e_name) values('" +
        //                    strCode + "',N'" + strTypeid + "','1',N'" + strCName + "',N'" + strEName + "')";
        //            iResult = sqlComm.ExecuteNonQuery();

        //            sqlComm.CommandText = "insert organizationstructure(parentid,hrid,typeid,orgcode) values('" +
        //               strparentid + "','1',N'" + strTypeid + "','1',N'" + strCode + "')";
        //            iResult = sqlComm.ExecuteNonQuery();

        //            if (iResult >= 1)
        //            {
        //                XmlNode retNode1 = SetXMLNode(retXML, "Result", strCode + " adding is OK");
        //                retNode.AppendChild(retNode1);
        //            }
        //            else
        //            {
        //                XmlNode retNode1 = SetXMLNode(retXML, "Result", strCode + " adding is failure");
        //                retNode.AppendChild(retNode1);
        //            }
        //        }
        //        #endregion

        //        #region
        //        sqlComm.CommandText = " update organizationstructure set orgcode5=org1.orgcode " +
        //                 " from organizationstructure inner join organizationstructure as org1 " +
        //                 " on organizationstructure.parentid =org1.structurecode and org1.typeid=5 and organizationstructure.typeid=6  ";
        //        iResult = sqlComm.ExecuteNonQuery();

        //        sqlComm.CommandText = "  update organizationstructure set orgcode4=org1.orgcode" +
        //                "  from organizationstructure inner join organizationstructure as org1 " +
        //                " on organizationstructure.parentid =org1.structurecode and org1.typeid=4 and organizationstructure.typeid=5 ";
        //        iResult = sqlComm.ExecuteNonQuery();

        //        sqlComm.CommandText = "  update organizationstructure set orgcode3=org1.orgcode" +
        //                "  from organizationstructure inner join organizationstructure as org1 " +
        //                " on organizationstructure.parentid =org1.structurecode and org1.typeid=3 and organizationstructure.typeid=4 ";
        //        iResult = sqlComm.ExecuteNonQuery();

        //        sqlComm.CommandText = " update organizationstructure set orgcode2=org1.orgcode" +
        //                " from organizationstructure inner join organizationstructure as org1 " +
        //                " on organizationstructure.parentid =org1.structurecode and org1.typeid=2 and organizationstructure.typeid=3 ";
        //        iResult = sqlComm.ExecuteNonQuery();

        //        sqlComm.CommandText = " update organizationstructure set orgcode1=org1.orgcode" +
        //                "  from organizationstructure inner join organizationstructure as org1 " +
        //                " on organizationstructure.parentid =org1.structurecode and org1.typeid=1 and organizationstructure.typeid=2 ";
        //        iResult = sqlComm.ExecuteNonQuery();

        //        sqlComm.CommandText = " update organizationstructure set orgcode6=orgcode where typeid=6; " +
        //                " update organizationstructure set orgcode5=orgcode where typeid=5; " +
        //                " update organizationstructure set orgcode4=orgcode where typeid=4; " +
        //                " update organizationstructure set orgcode3=orgcode where typeid=3; " +
        //                " update organizationstructure set orgcode2=orgcode where typeid=2; " +
        //                " update organizationstructure set orgcode1=orgcode where typeid=1 ";
        //        iResult = sqlComm.ExecuteNonQuery();
        //        #endregion
        //        sqlTran.Commit();
        //        return retXML.OuterXml;
        //    }
        //    catch (XmlException ex)
        //    {
        //        Console.Write(ex.Message);
        //        return null;
        //    }
        //}

        public DataTable GetDataSchema(string strTbl)
        {
            string strSql = string.Empty;
            SqlDataAdapter sqlData = new SqlDataAdapter();
            DataTable dtTable = new DataTable(strTbl);
            string strSQL = string.Empty;

            try
            {
                switch (strTbl)
                {
                    case "position":
                        sqlComm.CommandText = "select positioncode,c_name,e_name,jobdescription from position where 1=2";
                        break;
                    case "outsource":
                        sqlComm.CommandText = "select itemcode,c_name,e_name from parameters where 1=2 ";
                        break;
                    case "positionlevel":
                        sqlComm.CommandText = "select code,c_name,e_name from grade where 1=2 ";
                        break;
                    case "costcenter":
                        sqlComm.CommandText = "select code,c_name,e_name,category from costcenter  where 1=2";
                        break;
                    case "employee":
                        sqlComm.CommandText = "  select empcode,c_name,e_name,gender,ppissuecountry," +
                                     " employee1.id,marital,hiretype,nextreviewdate,email," +
                                     " pager,businessphone,mobile,raddress,homephone," +
                                     " rzipcode,origin,address1,dangan,e_sur_name," +
                                     " e_first_name,huko,bloodtype,hrstatus,birthday," +
                                     " Issuecountry,ppnumber,ppexpiredate,address2," +
                                     " employee1.hiredate,hrid,employee1.payrollid,servicestatus," +
                                     " hafid,pensionsequence,ccclass,employee1.hiresociety,base,baserate,origlocation,worklocation,silocation,taxlocation,effectivedate," +
                                     " nextchange,reason,totalscore,evaluationindex,senioritiesbefore," +
                                     " origempno,outsourceid,expectedreportingtime,structureid,entryproperty," +
                                     " attendancetype,probationlength,isehrentry,issendedpositive,workstructureid," +
                                     " interntype,learningminister,housingchargehotel,socialchargehotel,nccompany," +
                                     " weddingDate,highestdegree,highesttitle,currentcity," +
                                     " householdregistertype,passportduetime,passportissuetime,passportremark,workvisaduetime," +
                                     " residentpermitduedate,residentpermitissueplace,employmentduedate,employmentpermitissueplace,updateannualsalarydate," +
                                     " isunion,isabroad,initialannualleaveplan,initialsickleaveplan,adaccount," +
                                     " ncaccount,pmsaccount,adinitialpw,quitremark,bankid1 as bankaccountnumber, bankname1 as bankfullname,banktype,bankaddress,bankcurrency,bankaccountname," +
                                     " positioncode,currentposition,employee1.grade,positionstartdate,changereason,structureid,orgcode5 hotelcode, " +
                                     " onccode,costdepict,costcode,agencycode,agencyname,employee1.supervisor " +
                                     " from employee1 inner join employee3 on employee1.serialnumber =employee3.serialnumber " +
                                     " inner join employee4 on employee1.serialnumber =employee4.serialnumber " +
                                     " inner join empposition  on employee1.serialnumber =empposition.serialnumber  where 1=2  ";
                        break;
                   
                    case "quit":
                        sqlComm.CommandText = " select empcode,quitdate,quittype,quitreason, NextHired, NoHiredReason, QuitRemark " +
                                     " from employee1 inner join employee4 on employee1.serialnumber = employee4.serialnumber " +
                                      "  where 1=2";
                        break;

                    case "employeehotel":
                        sqlComm.CommandText = "  select '' changeid,empcode,disablity IsHotelChange,disablity IsPositionChange,"+
                             " disablity IsCostCenterChadrRow,disablity IsPositionLevel,disablity IsBasicSalaryChange,"+
                             " disablity IsWorkLocationChange,hiredate JobChangeEffectiveDate,hiredate SalaryEffectiveDate,"+
                             " businessphone SocialChargeHotel,businessphone Reason,hfcontribution base,"+
                             " businessphone positioncode,atsid orgcode,businessphone positionlevel,businessphone outsource," +
                             " businessphone costcenter,orgcode6 hotelcode from employee1 where 1=2";
                        break;

                    case "employeeoffice":
                        sqlComm.CommandText = "  select '' changeid,empcode,disablity IsSuperiorChange,disablity IsDepartChange,disablity IsPositionChange," +
                             " disablity IsCostCenterChadrRow,disablity IsPositionLevel,disablity IsBasicSalaryChange," +
                             " disablity IsWorkLocationChange,hiredate JobChangeEffectiveDate,hiredate SalaryEffectiveDate," +
                             " businessphone SocialChargeHotel,businessphone Reason,hfcontribution base," +
                             " businessphone positioncode,atsid orgcode,businessphone positionlevel,businessphone HousingChargeHotel,"+
                             " businessphone NCCompany,businessphone outsource,businessphone costcenter from employee1 where 1=2";
                        break;
                    case "monthimport":
                        sqlComm.CommandText = "  select empcode,code,amount from employeetotaldetail inner join employee1  " +
                           " on employeetotaldetail.serialnumber = employee1.serialnumber where 1=2 ";
                        break;
                    case "empbank":
                         sqlComm.CommandText = "  select empcode,bankid1 BankCard,bankname1 BankName,bankid2 BankType,bankid3 IsReimburse from employee1 " +
                            " inner join employee3 on employee3.serialnumber=employee1.serialnumber where 1=2";
                        break;
                    case "user":
                        sqlComm.CommandText = "  select usercode from users where 1=2";
                        break;
                }

                sqlData.SelectCommand = sqlComm;
                sqlData.Fill(dtTable);
                return dtTable;
            }
            catch (XmlException ex)
            {
                Console.Write(ex.Message);
                return dtTable;
            }
    

        }

        public DataTable AddMDM(DataTable dtTable)
        {
            string strSql = string.Empty;
            SqlDataAdapter sqlData = new SqlDataAdapter();
            DataSet dsData = new DataSet();
            
            int iResult = 0;
            DataTable retXML = new DataTable("Results");
            string strSerialNumber = "";

            try
            {
                if (dtTable == null || dtTable.Rows.Count <= 0)
                    return null;
                
                foreach (DataRow drRow in dtTable.Rows)
                {
                    SqlTransaction sqlTran = sqlConn.BeginTransaction();
                    sqlComm.Transaction = sqlTran;
                    sqlComm.CommandText = "select replace(REPLICATE('X',10-len(max(serialnumber)+1))+convert(char,max(serialnumber)+1),'X','0') " +
                        " from employee1 where len(serialnumber)=10";
                    sqlData.SelectCommand = sqlComm;
                    dsData = new DataSet();
                    sqlData.Fill(dsData);
                    if (dsData == null)
                        strSerialNumber = "0000000001";
                    else
                        strSerialNumber = dsData.Tables[0].Rows[0][0].ToString().Substring(0, 10);


                    string strhotelno = GetNodeValue(drRow, "hotelno", 100);

                    dsData = new DataSet();
                    sqlComm.CommandText = "select serialnumber from employee1 where empcode='" + strhotelno + "'";
                    dsData = new DataSet();
                    sqlData.SelectCommand = sqlComm;
                    sqlData.Fill(dsData);

                    string strhotelname = GetNodeValue(drRow, "hotelname", 100);
                    string strhotelunifyno = GetNodeValue(drRow, "hotelunifyno", 100);
                    string strcityareano = GetNodeValue(drRow, "cityareano", 100);
                    string strcityname = GetNodeValue(drRow, "cityname", 100);
                    string strprovincename = GetNodeValue(drRow, "provincename", 100);
                    string strprincipal= GetNodeValue(drRow, "principal", 100);
                    
                    string stropeningdate = GetNodeDate(drRow, "openingdate");
                    string sttrialopeningdate = GetNodeDate(drRow, "trialopeningdate");
                    string strappraisalday= GetNodeValue(drRow, "appraisalday", 100);
                    string strcityareaname= GetNodeValue(drRow, "cityareaname", 100);
                    string strbrandid= GetNodeValue(drRow, "brandid", 100);

                    string strhrid = "1";
                    string strpayrollid = "12";
                    string servicestatus = "1";
                    string employeeflag = "1";                    
                    

                    if (dsData != null && dsData.Tables.Count > 0 && dsData.Tables[0].Rows.Count > 0)
                    {

                    }
                    else
                    {
                        //employee1            
                        sqlComm.CommandText = "insert employee1(serialnumber,empcode,c_name,e_name,address2," + System.Environment.NewLine +
                            "christian_name,huko,pinyin,hiredate,hirevalid," + System.Environment.NewLine +
                            "hiresociety,hrid,servicestatus,payrollcode,systartdate," + System.Environment.NewLine +
                            "employeeflag,payrollid,orgstartdate,payrolleffectivedate " + System.Environment.NewLine +
                             QuoNStr(strSerialNumber) + "," + QuoNStr(strhotelno) + "," + QuoNStr(strhotelname) + "," + QuoNStr(strhotelunifyno) + "," + QuoNStr(strcityareano) +  "," + System.Environment.NewLine +
                            QuoNStr(strcityname) + "," + QuoNStr(strprovincename) + "," + QuoNStr(strprincipal) + "," + QuoNStr(stropeningdate) + "," + QuoNStr(sttrialopeningdate) + "," + System.Environment.NewLine +
                            QuoNStr(strappraisalday) + ",1,1,'PAYJD3',1," + System.Environment.NewLine +
                            "1,12," + QuoNStr(stropeningdate) + "," + QuoNStr(stropeningdate) + ")";
                        iResult = sqlComm.ExecuteNonQuery();                  

                        sqlComm.CommandText = "insert employee2(serialnumber) values('" + strSerialNumber + "')";
                        iResult = sqlComm.ExecuteNonQuery();

                        sqlComm.CommandText = "insert employee3(serialnumber,payrollid,status,emptype,step,benefit,onpayroll,allowanceclass,homecurrency,currencyclass) values(" +
                            QuoNStr(strSerialNumber) + ",'12','01','01','3','1','0','Allowance','RMB','RMB')";
                        iResult = sqlComm.ExecuteNonQuery();



                        sqlComm.CommandText = "insert employee4(serialnumber,cityareaname,hoteladdress,operatemodeid,official," + System.Environment.NewLine +
                            "brandid,operation,onccode,rooms,operatescopeid," + System.Environment.NewLine +
                            "assessment,finadaccount,isdelete,pmsbranchcode,cityno," + System.Environment.NewLine +
                            "hotelstate,projectname,developno,provinceno,countryno," + System.Environment.NewLine +
                            "countryname) values('" +
                             QuoNStr(strSerialNumber) + "," + QuoNStr(strcityareaname) + "," + QuoNStr(strhoteladdress) + "," + QuoNStr(stroperatemodeid) + "," + QuoNStr(strofficial) + "," + System.Environment.NewLine +
                             QuoNStr(strbrandid) + "," + QuoNStr(stroperation) + "," + QuoNStr(stronccode) + "," + QuoNStr(strrooms) + "," + QuoNStr(stroperatescopeid) + "," + System.Environment.NewLine +
                             QuoNStr(strassessment) + "," + QuoNStr(strfinadaccount) + "," + QuoNStr(strisdelete) + "," + QuoNStr(strpmsbranchcode) + "," + QuoNStr(strcityno) + "," + System.Environment.NewLine +
                             QuoNStr(strhotelstate) + "," + QuoNStr(strprojectname) + "," + QuoNStr(strdevelopno) + "," + QuoNStr(strprovinceno) + "," + QuoNStr(strcountryno) + "," + System.Environment.NewLine +
                             QuoNStr(strcountryname) + ")";

                        iResult = sqlComm.ExecuteNonQuery();

                        sqlComm.CommandText = "insert empposition(serialnumber,positionstartdate,positioncode,currentposition) values(" +
                        QuoNStr(strSerialNumber) + "," + QuoNStr(stropeningdate) + ",'DZ','1')";
                        iResult = sqlComm.ExecuteNonQuery();    
                    }

                    if (iResult >= 1)
                    {
                        retXML = GetResultTable(retXML, true, strhotelno, "Adding is OK", true);
                        sqlTran.Commit();
                    }
                    else
                    {
                        retXML = GetResultTable(retXML, false, strhotelno, "Adding is failure", true);
                        sqlTran.Rollback();
                    }
                    
                }
                                
                return retXML;
            }
            catch (XmlException ex)
            {
                
                retXML = GetResultTable(retXML, false, "All", "Adding is failure", true);
                return retXML;
            }
        }
        

        public DataTable GetMonthDict()
        {
            string strSql = string.Empty;
            SqlDataAdapter sqlData = new SqlDataAdapter();
            DataTable dtTable = new DataTable("DataDict");
            string strSQL = string.Empty;

            try
            {

                sqlComm.CommandText = "select 'SALARY' as codetype,catecode code,c_name from paycategory union all " +
                        "select 'LV',code,c_name from leavetype  union  all " +
                        "select 'OT',code,c_name from ottype union all " +
                        "select 'SHIFT',code,c_name from Shifttype ";          

                sqlData.SelectCommand = sqlComm;
                sqlData.Fill(dtTable);
                return dtTable;
            }
            catch (XmlException ex)
            {
                Console.Write(ex.Message);
                return dtTable;
            }


        }        
        
        public DataTable GetOrganizationData()
        {
            string strSql = string.Empty;
            SqlDataAdapter sqlData = new SqlDataAdapter();
            DataTable dsData = new DataTable("organization");
            int iResult = 0;

            try
            {
                sqlComm.CommandText = " select structurecode,orgcode,b.c_name,b.e_name,a.typeid," +
                        " orgcode1,isnull(b1.c_name,'')org1cname ,isnull(b1.e_name,'') org1ename," +
                        " orgcode2,isnull(b2.c_name,'')org2cname ,isnull(b2.e_name,'') org2ename," +
                        " orgcode3,isnull(b3.c_name,'')org3cname ,isnull(b3.e_name,'') org3ename," +
                        " orgcode4,isnull(b4.c_name,'')org4cname ,isnull(b4.e_name,'') org4ename," +
                        " orgcode5,isnull(b5.c_name,'')org5cname ,isnull(b5.e_name,'') org5ename," +
                        " orgcode6,isnull(b6.c_name,'')org6cname ,isnull(b6.e_name,'') org6ename" +
                        "  from organizationstructure a left join organization b on a.orgcode = b.code and a.typeid =b.typeid" +
                        "  left join organization b1 on a.orgcode1 = b1.code and b1.typeid=1" +
                        " left join organization b2 on a.orgcode2 = b2.code and b2.typeid=2" +
                        " left join organization b3 on a.orgcode3 = b3.code and b3.typeid=3" +
                        " left join organization b4 on a.orgcode4 = b4.code and b4.typeid=4" +
                        " left join organization b5 on a.orgcode5= b5.code and b5.typeid=5" +
                        " left join organization b6 on a.orgcode6 = b6.code and b6.typeid=6 " ;
                iResult = sqlComm.ExecuteNonQuery();              
                sqlData.SelectCommand = sqlComm;
                sqlData.Fill(dsData);
                return dsData;

            }
            catch (XmlException ex)
            {
                Console.Write(ex.Message);
                return null;
            }

        }

        public DataTable GetEmployeeData(string strBeginDate, string strEndDate)
        {
            string strSql = string.Empty;
            SqlDataAdapter sqlData = new SqlDataAdapter();
            DataTable dsData = new DataTable("employee");

            try
            {
                sqlComm.CommandText = "select employee1.empcode,newdata,notes,validdate,mod_user,mod_time,tablename,fieldname  from fieldaudit inner join employee1 " +
                     " on fieldaudit.serialnumber=employee1.serialnumber where tablename in ('employee1','employee3','employee4')" +
                     " and fieldname not in('payrollid','empcode','employeeflag','servicestatus') and mod_user<>'HuazhuOA' and  mod_time>='" + strBeginDate + "' and mod_time<'" + strEndDate + "'";
                sqlData.SelectCommand = sqlComm;
                sqlData.Fill(dsData);
                return dsData;

            }
            catch (XmlException ex)
            {
                Console.Write(ex.Message);
                return null;
            }

        }

        public DataTable GetPositionHistoryData(string strEmp, string strBeginDate, string strEndDate)
        {
            string strSql = string.Empty;
            SqlDataAdapter sqlData = new SqlDataAdapter();
            DataTable dsData = new DataTable("positionhistory");

            try
            {
                sqlComm.CommandText = "select empcode,positioncode,c_title,e_title,jobdescription,positionstartdate,changedate" +
                        " from empposition inner join employee1 on empposition.serialnumber=employee1.serialnumber" +
                        " where empcode=N'" + strEmp + "' and positionstartdate>='" + strBeginDate + "' and positionstartdate<='" + strEndDate + "'";
                sqlData.SelectCommand = sqlComm;
                sqlData.Fill(dsData);
                return dsData;

            }
            catch (XmlException ex)
            {
                Console.Write(ex.Message);
                return null;
            }

        }

        public DataTable GetSalaryChangeData(string strEmp, string strBeginDate, string strEndDate)
        {
            string strSql = string.Empty;
            SqlDataAdapter sqlData = new SqlDataAdapter();
            DataTable dsData = new DataTable("salarychange");

            try
            {
                sqlComm.CommandText = "select empcode,paystartdate,payenddate,paychangedate,baserate from salaryhistory " +
                        "  inner join employee1 on salaryhistory.serialnumber=employee1.serialnumber " +
                        " where empcode=N'" + strEmp + "' and paystartdate>='" + strBeginDate + "' and paystartdate<='" + strEndDate + "'";
                sqlData.SelectCommand = sqlComm;
                sqlData.Fill(dsData);
                return dsData;

            }
            catch (XmlException ex)
            {
                Console.Write(ex.Message);
                return null;
            }

        }

        public DataTable SetEmpBankCard(DataTable dtTable)
        {
            string strSql = string.Empty;
            SqlDataAdapter sqlData = new SqlDataAdapter();
            DataSet dsData = new DataSet();
            SqlTransaction sqlTran;
            sqlTran = sqlConn.BeginTransaction();
            sqlComm.Transaction = sqlTran;
            int iResult = 0;
            DataTable retXML = new DataTable("Results");

            try
            {
                foreach (DataRow dtRow in dtTable.Rows)
                {
                    string strEmpCode = GetNodeValue(dtRow, "empcode");
                    string strBankType = GetNodeValue(dtRow, "BankType");
                    string strIsReimburse = GetNodeValue(dtRow, "IsReimburse");
                    string strBankCard = GetNodeValue(dtRow, "BankCard", 30);
                    string strBankName = GetNodeValue(dtRow, "BankName", 100);

                    sqlComm.CommandText = "select serialnumber from employee1 where empcode=" + QuoNStr(strEmpCode);
                    sqlData.SelectCommand = sqlComm;
                    dsData = new DataSet();
                    sqlData.Fill(dsData);

                    if (dsData == null || dsData.Tables.Count <= 0 || dsData.Tables[0].Rows.Count <= 0)
                    {
                        retXML = GetResultTable(retXML, false, strEmpCode, "Not exist", true);
                        continue;
                    }

                    if(strBankType=="1" && strIsReimburse=="1")
                        sqlComm.CommandText = "update employee3 set bankid1=" + QuoNStr(strBankCard) + ",bankname1=" + QuoNStr(strBankName) + 
                           " ,bankid2=" +QuoNStr(strBankCard) + ",bankname2=" +QuoNStr(strBankName) + " from employee1 " +
                           " inner join employee3 on employee3.serialnumber=employee1.serialnumber " + 
                           " where empcode=" + QuoNStr(strEmpCode);

                    if (strBankType == "1" && strIsReimburse == "0")
                        sqlComm.CommandText = "update employee3 set bankid1=" + QuoNStr(strBankCard) + ",bankname1=" + QuoNStr(strBankName) + " from employee1 " +
                           " inner join employee3 on employee3.serialnumber=employee1.serialnumber " +
                           " where empcode=" + QuoNStr(strEmpCode);

                    if (strBankType == "2" )
                        sqlComm.CommandText = "update employee3 set bankid2=" + QuoNStr(strBankCard) + ",bankname2=" + QuoNStr(strBankName) + " from employee1 " +
                           " inner join employee3 on employee3.serialnumber=employee1.serialnumber " +
                           " where empcode=" + QuoNStr(strEmpCode);

                    iResult = sqlComm.ExecuteNonQuery();

                    if (iResult >= 1)
                    {
                        retXML = GetResultTable(retXML, true, strEmpCode, "Change BankCard is OK", true);
                    }
                    else
                    {
                        retXML = GetResultTable(retXML, false, strEmpCode, "Change BankCard is failure", true);
                    }
                }
        
                sqlTran.Commit();

                return retXML;
            }
            catch (XmlException ex)
            {
                sqlTran.Rollback();
                Console.Write(ex.Message);
                return null;
            }

        }
        
        public DataTable GetAllEmpBankCard()
        {
            string strSql = string.Empty;
            SqlDataAdapter sqlData = new SqlDataAdapter();
            DataTable dsData = new DataTable("Bank");

            try
            {
                sqlComm.CommandText = "select empcode,bankid1,bankid2,bankname1,bankname2,AcountHolder1,currencyclass,paytype,payfrequency from employee1 " +
                          "  inner join employee3 on employee3.serialnumber=employee1.serialnumber " +
                          "  inner join employee4 on employee4.serialnumber=employee1.serialnumber " +
                        " where employee1.serialnumber in(select serialnumber from [All_Active_Employees])";
                sqlData.SelectCommand = sqlComm;
                sqlData.Fill(dsData);
                return dsData;

            }
            catch (XmlException ex)
            {
                Console.Write(ex.Message);
                return null;
            }
        }

        public DataTable GetEmpBankCard(string strEmp)
        {
            string strSql = string.Empty;
            SqlDataAdapter sqlData = new SqlDataAdapter();
            DataTable dsData = new DataTable("Bank");

            try
            {
//                employee4.paytype  nvarchar(100)   付款类型
//employee4.payfrequency nvarchar(100)  付款周期

//employee3.bankname1  nvarchar(100)   开户银行
//employee3.AcountHolder1  nvarchar(250) 户名
//employee3.currencyclass nvarchar(10) 货币类型

                sqlComm.CommandText = "select empcode,bankid1,bankid2,bankname1,bankname2,AcountHolder1,currencyclass,paytype,payfrequency from employee1 " +
                        "  inner join employee3 on employee3.serialnumber=employee1.serialnumber " +
                        "  inner join employee4 on employee4.serialnumber=employee1.serialnumber " +
                        " where empcode=" + QuoNStr(strEmp) ;
                sqlData.SelectCommand = sqlComm;
                sqlData.Fill(dsData);
                return dsData;

            }
            catch (XmlException ex)
            {
                Console.Write(ex.Message);
                return null;
            }
        }

        public DataTable GetTransTable(DataTable dtSource)
        {
            string strSql = string.Empty;
            SqlDataAdapter sqlData = new SqlDataAdapter();
            DataTable dsData = new DataTable("salaryhistory");
            DataTable dsFields = new DataTable("fields");

            try
            {
                sqlComm.CommandText = "select fieldcode,c_name from fields where tablecode='employee2' group by fieldcode,c_name union all " +
                            " select fieldcode,c_name from fields where  tablecode='employee3' and fieldcode in('iitbase','net','iit')  union all " +
                            " select fieldcode,c_name from fields where  tablecode='employee5' and fieldcode in('selfdef0058','selfdef0060')";
                sqlData.SelectCommand = sqlComm;
                sqlData.Fill(dsFields);

                DataColumn dcCol1 = new DataColumn("工号", typeof(String));
                dsData.Columns.Add(dcCol1);
                DataColumn dcCol2 = new DataColumn("薪资期段", typeof(String));
                dsData.Columns.Add(dcCol2);
                DataColumn dcCol3 = new DataColumn("薪资项编码", typeof(String));
                dsData.Columns.Add(dcCol3);
                DataColumn dcCol4 = new DataColumn("薪资项名称", typeof(String));
                dsData.Columns.Add(dcCol4);
                DataColumn dcCol5 = new DataColumn("金额", typeof(Double));
                dsData.Columns.Add(dcCol5);
                
                foreach(DataRow drRow in dtSource.Rows)
                {
                    for (int i = 3; i < dtSource.Columns.Count; i++)
                    {
                        DataRow drNew = dsData.NewRow();
                        drNew[0] = drRow[0].ToString();
                        drNew[1] = drRow[2].ToString();

                        drNew[2] = dtSource.Columns[i].ColumnName;

                        DataRow[] drRows=dsFields.Select("fieldcode='" + dtSource.Columns[i].ColumnName +"'");
                        if(drRows.Length>=1)
                            drNew[3] = drRows[0][1].ToString();

                        drNew[4] = drRow[i].ToString();
                        dsData.Rows.Add(drNew);
                    }                    
                }

                return dsData;

            }
            catch (XmlException ex)
            {
                Console.Write(ex.Message);
                return null;
            }

        }
        
        public DataTable GetSalaryHistorySum(string strEmp, string strBeginDate, string strEndDate)
        {
            string strSql = string.Empty;
            SqlDataAdapter sqlData = new SqlDataAdapter();
            DataTable dsData = new DataTable("salaryhistory");
            DataTable dsSalary = new DataTable("salary");

            try
            {
                sqlComm.CommandText = "select empcode,history_employee3.perioddate,selfdef0058,selfdef0060,net from  employee1 " +
                        "inner join history_employee5 on employee1.serialnumber=history_employee5.serialnumber " +
                        "inner join history_employee3 on employee1.serialnumber=history_employee3.serialnumber " +
                        "and history_employee5.serialnumber=history_employee3.serialnumber " +
                        "and  history_employee5.perioddate=history_employee3.perioddate " +
                        " where empcode=N'" + strEmp + "' and history_employee3.perioddate>='" + strBeginDate + "' and history_employee3.perioddate<='" + strEndDate + "'";
                sqlData.SelectCommand = sqlComm;
                sqlData.Fill(dsSalary);
                dsData = GetTransTable(dsSalary);

                return dsData;

            }
            catch (XmlException ex)
            {
                Console.Write(ex.Message);
                return null;
            }


        }
        public DataTable GetSalaryHistoryData(string strEmp, string strBeginDate, string strEndDate)
        {
            string strSql = string.Empty;
            SqlDataAdapter sqlData = new SqlDataAdapter();
            DataTable dsData = new DataTable("salaryhistory");
            DataTable dsSalary = new DataTable("salary");

            try
            {
                sqlComm.CommandText = "select empcode,history_employee2.*,iitbase,net,iit from  employee1 " +
                        "inner join history_employee2 on employee1.serialnumber=history_employee2.serialnumber " +
                        "inner join history_employee3 on employee1.serialnumber=history_employee3.serialnumber " +
                        "and history_employee2.serialnumber=history_employee3.serialnumber " +
                        "and  history_employee2.perioddate=history_employee3.perioddate " +
                        " where empcode=N'" + strEmp + "' and history_employee2.perioddate>='" + strBeginDate + "' and history_employee2.perioddate<='" + strEndDate + "'";
                sqlData.SelectCommand = sqlComm;
                sqlData.Fill(dsSalary);
                dsData = GetTransTable(dsSalary);

                return dsData;

            }
            catch (XmlException ex)
            {
                Console.Write(ex.Message);
                return null;
            }

        }

        public DataTable SetEmployeeQuit(DataTable dtTable)
        {
            string strSql = string.Empty;
            SqlDataAdapter sqlData = new SqlDataAdapter();
            DataSet dsData = new DataSet();
            SqlTransaction sqlTran;
            sqlTran = sqlConn.BeginTransaction();
            sqlComm.Transaction = sqlTran;
            int iResult = 0;
            DataTable retXML = new DataTable("Results");
            string strSerialNumber = string.Empty;
            string strEmpcode;
            string strQuitDate;
            string strQuitType;
            string strQuitReason;

//            NextHired     是否可在录用
//NoHiredReason 不可录用原因
//QuitRemark 离职备注

            try
            {
                if (dtTable == null || dtTable.Rows.Count <= 0)
                {
                    retXML = GetResultTable(retXML, false, "All", "No Data", true);
                    return retXML;
                }

                  foreach (DataRow drRow in dtTable.Rows)
                  {
                       strEmpcode=drRow["empcode"].ToString();
                       strQuitDate=GetNodeDate(drRow,"quitdate");
                       strQuitType=drRow["quittype"].ToString();
                       strQuitReason=drRow["quitreason"].ToString();


                      sqlComm.CommandText = "select serialnumber,quitdate from employee1 where empcode='" + strEmpcode + "'";
                      sqlData.SelectCommand = sqlComm;
                      dsData = new DataSet();
                      sqlData.Fill(dsData);

                      if (dsData == null || dsData.Tables.Count <= 0 || dsData.Tables[0].Rows.Count <= 0)
                      {
                          retXML = GetResultTable(retXML, false, strEmpcode, "Empcode isn't exist", true);
                          continue;
                      }
                      strSerialNumber = dsData.Tables[0].Rows[0][0].ToString();

                       if (dsData.Tables[0].Rows[0]["quitdate"] != DBNull.Value)
                      {
                          retXML = GetResultTable(retXML, false, strEmpcode, "Employee has already quit", true);
                          continue;
                      }
                      strSerialNumber = dsData.Tables[0].Rows[0][0].ToString();

                      sqlComm.CommandText = "  insert empquit(serialnumber,lasthiredate,quitdate,quitnoticedate,quitleavedate,quittype," +
                          "quitpensiondate,quithafdate,quitfiledate,quitreason) values(" +
                           QuoNStr(strSerialNumber) + ",(select hiredate from employee1 where serialnumber=" +
                           QuoNStr(strSerialNumber) + ")," + QuoNStr(strQuitDate) + "," + QuoNStr(strQuitDate) + "," + QuoNStr(strQuitDate) + "," +
                            QuoNStr(strQuitType) + "," + QuoNStr(strQuitDate) + "," + QuoNStr(strQuitDate) + "," + QuoNStr(strQuitDate) + "," +
                            QuoNStr(strQuitReason) + ")";
                      iResult = sqlComm.ExecuteNonQuery();

                      sqlComm.CommandText = "  insert termination(serialnumber,newquitdate,quitnoticedate,quitleavedate,quittype," +
                         "quitpensiondate,quithafdate,quitfiledate,quitreason) values(" +
                          QuoNStr(strSerialNumber) + "," + QuoNStr(strQuitDate) + "," + QuoNStr(strQuitDate) + "," + QuoNStr(strQuitDate) + "," +
                           QuoNStr(strQuitType) + "," + QuoNStr(strQuitDate) + "," + QuoNStr(strQuitDate) + "," + QuoNStr(strQuitDate) + "," +
                           QuoNStr(strQuitReason) + ")";
                      iResult = sqlComm.ExecuteNonQuery();

                      sqlComm.CommandText = " update  employee1 set quitdate=" + QuoNStr(strQuitDate) + "," +
                           " quitnoticedate=" + QuoNStr(strQuitDate) + "," +
                           " quitleavedate=" + QuoNStr(strQuitDate) + "," +
                           " quitpensiondate=" + QuoNStr(strQuitDate) + "," +
                           " quithafdate=" + QuoNStr(strQuitDate) + "," +
                           " quitfiledate=" + QuoNStr(strQuitDate) + "," +
                           " quittype=" + QuoNStr(strQuitType) + "," +
                           " quitreason=" + QuoNStr(strQuitReason) +
                           " where serialnumber=" + QuoNStr(strSerialNumber);
                      iResult = sqlComm.ExecuteNonQuery();

                      string strNextHired = GetNodeBool(drRow,"nexthired");
                      string strNoHiredReason=drRow["nohiredreason"].ToString();
                      string strQuitRemark = drRow["quitremark"].ToString();

                      sqlComm.CommandText = " update  employee4 set nexthired=" + QuoNStr(strNextHired) + "," +
                           " nohiredreason=" + QuoNStr(strNoHiredReason) + "," +
                           " quitremark=" + QuoNStr(strQuitRemark) +
                           " where serialnumber=" + QuoNStr(strSerialNumber);
                      iResult = sqlComm.ExecuteNonQuery();

                      if (iResult >= 1)
                      {
                          retXML = GetResultTable(retXML, true, strEmpcode, "quit is OK", true);
                      }
                      else
                      {
                          retXML = GetResultTable(retXML, false, strEmpcode, "quit is failure", true);
                      }
                }
                  
            
                sqlTran.Commit();
                return retXML;
            }
            catch (XmlException ex)
            {
                sqlTran.Rollback();
                Console.Write(ex.Message);
                return retXML;
            }
        }

        public DataTable CloseUser(DataTable dtTable)
        {
            string strSql = string.Empty;
            SqlDataAdapter sqlData = new SqlDataAdapter();
            DataSet dsData = new DataSet();
            SqlTransaction sqlTran;
            sqlTran = sqlConn.BeginTransaction();
            sqlComm.Transaction = sqlTran;
            int iResult = 0;
            DataTable retXML = new DataTable("Results");

            try
            {
                foreach (DataRow dtRow in dtTable.Rows)
                {
                    string strUserCode = GetNodeValue(dtRow, "usercode");

                    sqlComm.CommandText = "select * from users where usercode=" + QuoNStr(strUserCode);
                    sqlData.SelectCommand = sqlComm;
                    dsData = new DataSet();
                    sqlData.Fill(dsData);

                    if (dsData == null || dsData.Tables.Count <= 0 || dsData.Tables[0].Rows.Count <= 0)
                    {
                        retXML = GetResultTable(retXML, false, strUserCode, " Not exist", true);
                        continue;
                    }

                    sqlComm.CommandText = "update users set lockflag=1 where usercode=" + QuoNStr(strUserCode);

                 
                    iResult = sqlComm.ExecuteNonQuery();

                    if (iResult >= 1)
                    {
                        retXML = GetResultTable(retXML, true, strUserCode, " Lock User is OK", true);
                    }
                    else
                    {
                        retXML = GetResultTable(retXML, false, strUserCode, " Lock User is failure", true);
                    }
                }

                sqlTran.Commit();

                return retXML;
            }
            catch (XmlException ex)
            {
                sqlTran.Rollback();
                Console.Write(ex.Message);
                return null;
            }

        }
    }

}
