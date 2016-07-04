using System;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Dynamic;
using System.Text;
using System.Text.RegularExpressions;
using System.Globalization;
using System.Threading.Tasks;
using System.Runtime.InteropServices;
using System.Net;
using System.Net.Mail;
using System.Net.Http;
using System.Net.Http.Headers;
using System.Web;
using System.Web.Security;
using System.Data;
using System.Data.Odbc;
using System.Data.SqlClient;
using System.Collections.Specialized;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;
using System.Reflection;
using otrsrest;
using System.Security.Cryptography;

namespace moodlerest
{
    public static class otrssettings
    {
        public static string customer { get; set; }
        public static string errqueue { get; set; }
        public static string dupqueue { get; set; }
    }

    public class user
    {
        // Object to hold user data.

        public string firstname { get; set; }
        public string middlename { get; set; }
        public string lastname { get; set; }
        private string _email;
        public string alternatename { get; set; }
        public string city { get; set; }
        public string country { get; set; }
        public string candidate_number { get; set; }
        public string sapcustomernumber { get; set; }
        public string sapsuppliernumber { get; set; }
        public string app_number { get; set; }
        public string password { get; set; }
        public bool generate_password { get; set; }
        public bool force_password_change { get; set; }
        public string institution { get; set; }
        public string address { get; set; }
        public string phone1 { get; set; }
        public string phone2 { get; set; }
        public string url { get; set; }

        public string email
        {
            get
            {
                return _email;
            }
            set
            {
                _email = value.ToLower();
            }
        }

        public user()
        {
            // Set preference defaults.
            generate_password = true;
            force_password_change = true;
        }

    }


    public static class UpdateSettings
    {
        public static bool Update(string _name, string _value)
        {
            // Function to te stored login details for the Moodle connector.

            if (_name == "MoodleToken")
            {
                // Encode tokens for storage
                byte[] unenc = Encoding.Unicode.GetBytes(_value);

                // Generate additional entropy (will be used as the Initialization vector)
                byte[] entropy = new byte[20];
                using (RNGCryptoServiceProvider rng = new RNGCryptoServiceProvider())
                {
                    rng.GetBytes(entropy);
                }

                byte[] enc = ProtectedData.Protect(unenc, entropy, DataProtectionScope.CurrentUser);

                Properties.moodlerest.Default.MoodleToken = System.Convert.ToBase64String(enc);
                Properties.moodlerest.Default.MEntropy = System.Convert.ToBase64String(entropy);
                Properties.moodlerest.Default.Save();
                return true;
            }
            else if (_name == "GeoPass")
            {
                // Encode passwords for storage
                byte[] unenc = Encoding.Unicode.GetBytes(_value);

                // Generate additional entropy (will be used as the Initialization vector)
                byte[] entropy = new byte[20];
                using (RNGCryptoServiceProvider rng = new RNGCryptoServiceProvider())
                {
                    rng.GetBytes(entropy);
                }

                byte[] enc = ProtectedData.Protect(unenc, entropy, DataProtectionScope.CurrentUser);

                Properties.moodlerest.Default.GeoPass = System.Convert.ToBase64String(enc);
                Properties.moodlerest.Default.GEntropy = System.Convert.ToBase64String(entropy);
                Properties.moodlerest.Default.Save();
                return true;
            }
            else if (_name == "DBPass")
            {
                // Encode passwords for storage
                byte[] unenc = Encoding.Unicode.GetBytes(_value);

                // Generate additional entropy (will be used as the Initialization vector)
                byte[] entropy = new byte[20];
                using (RNGCryptoServiceProvider rng = new RNGCryptoServiceProvider())
                {
                    rng.GetBytes(entropy);
                }

                byte[] enc = ProtectedData.Protect(unenc, entropy, DataProtectionScope.CurrentUser);

                Properties.moodlerest.Default.DBPass = System.Convert.ToBase64String(enc);
                Properties.moodlerest.Default.DBEntropy = System.Convert.ToBase64String(entropy);
                Properties.moodlerest.Default.Save();
                return true;
            }
            else if (_name != "GEntropy" && _name != "MEntropy" && _name != "DBEntropy" && Properties.moodlerest.Default[_name] != null)
            {
                // If setting exists update it.
                Properties.moodlerest.Default[_name] = _value;
                Properties.moodlerest.Default.Save();
                return true;
            }
            else
            {
                return false;
            }
        }

        public static string Get(string _name)
        {
            if (_name != "MoodleToken" && _name != "MEntropy" && _name != "GeoPass" && _name != "GEntropy" && _name != "DBPass" && _name != "DBEntropy" && Properties.moodlerest.Default[_name] != null)
            {
                return Properties.moodlerest.Default[_name].ToString();
            }
            else
            {
                return "";
            }
        }
    }

    public class lkcriteria
    {
        // Object to hold criteria for looking up users.

        public string key { get; set; }
        public string value { get; set; }
    }

    public class lkupidnumber
    {
        // Object to hold full lookup users request.

        public string wsfunction { get; set; }
        public List<lkcriteria> criteria { get; set; }

        public lkupidnumber()
        {
            wsfunction = "local_extrauserlookups_get_users";
            criteria = new List<lkcriteria>();
        }
    }

    public class muser
    {
        public string id { get; set; }
        public string username { get; set; }
        public string firstname { get; set; }
        public string lastname { get; set; }
        public string middlename { get; set; }
        public string idnumber { get; set; }
        public string email { get; set; }
        public string candidate_number { get; set; }
        public string sapcustomernumber { get; set; }
        public string sapsuppliernumber { get; set; }
    }

    public class EmailCheck
    {
        // Check strings are valid email addresses.

        private bool invalid;

        public EmailCheck()
        {
            invalid = false;
        }

        public bool IsValidEmail(string strIn)
        {
            invalid = false;
            if (String.IsNullOrEmpty(strIn))
                return false;

            // Use IdnMapping class to convert Unicode domain names.
            strIn = Regex.Replace(strIn, @"(@)(.+)$", this.DomainMapper);
            if (invalid)
                return false;

            // Return true if strIn is in valid e-mail format. 
            return Regex.IsMatch(strIn,
                   @"^(?("")(""[^""]+?""@)|(([-!#\$%&'\*\+/=\?\^`\{\}\|~\w]((\.(?!\.))|[-!#\$%&'\*\+/=\?\^`\{\}\|~\w])*)(?<=[-!#\$%&'\*\+/=\?\^`\{\}\|~\w])@))" +
                   @"(?(\[)(\[(\d{1,3}\.){3}\d{1,3}\])|(([0-9a-z][-\w]*[0-9a-z]*\.)+[a-z0-9]{2,17}))$",
                   RegexOptions.IgnoreCase);
        }

        private string DomainMapper(Match match)
        {
            // IdnMapping class with default property values.
            IdnMapping idn = new IdnMapping();

            string domainName = match.Groups[2].Value;
            try
            {
                domainName = idn.GetAscii(domainName);
            }
            catch (ArgumentException)
            {
                invalid = true;
            }
            return match.Groups[1].Value + domainName;
        }
    }

    public class CheckForUser
    {
        // Class to check if a user already exists

        public Dictionary<user, JToken> duplicates { get; set; } // Dictionary of users that appear to be duplicates (including error JTokens)
        public Dictionary<user, String> existingusers { get; set; } // Dictionary of users that already exist (to lookup ids)
        public List<user> newusers { get; set; }  // List of users that are not currently on system (to be added)
        public OCWebRequest moodle { get; set; }
        private IEnumerable<muser> musers;

        public CheckForUser()
        {
            // Initialise storage objects.

            duplicates = new Dictionary<user, JToken>();
            existingusers = new Dictionary<user, string>();
            newusers = new List<user>();
            moodle = new OCWebRequest();
            Console.WriteLine("Getting Moodle Users");
            DataTable rawmusers = GetUsers.MoodleODBC("SELECT id, username, idnumber, email, firstname, middlename, lastname FROM mdl_user WHERE deleted = 0");
            DataTable cfields = GetUsers.MoodleODBC("SELECT mdl_user_info_field.shortname, mdl_user_info_data.userid, mdl_user_info_data.data FROM mdl_user_info_field INNER JOIN mdl_user_info_data ON mdl_user_info_field.id = mdl_user_info_data.fieldid WHERE mdl_user_info_field.shortname='SAPC' OR mdl_user_info_field.shortname='SAPS' OR mdl_user_info_field.shortname='candno'");

            musers = from u in rawmusers.AsEnumerable()
                     join cf1 in cfields.AsEnumerable().Where(t => t["shortname"].ToString() == "candno") on u["id"].ToString() equals cf1["userid"].ToString() into candnos
                     from cn in candnos.DefaultIfEmpty()
                     join cf2 in cfields.AsEnumerable().Where(s => s["shortname"].ToString() == "SAPC") on u["id"].ToString() equals cf2["userid"].ToString() into sapcs
                     from sapc in sapcs.DefaultIfEmpty()
                     join cf3 in cfields.AsEnumerable().Where(u => u["shortname"].ToString() == "SAPS") on u["id"].ToString() equals cf3["userid"].ToString() into sapss
                     from saps in sapss.DefaultIfEmpty()
                     select new muser { id = u["id"].ToString(), username = u["username"].ToString(), firstname = u["firstname"].ToString(), middlename = u["middlename"].ToString(), lastname = u["lastname"].ToString(), idnumber = u["idnumber"].ToString(), email = u["email"].ToString(), candidate_number = (cn == null ? String.Empty : cn["data"].ToString()), sapcustomernumber = (sapc == null ? String.Empty : sapc["data"].ToString()), sapsuppliernumber = (saps == null ? String.Empty : saps["data"].ToString()) };

            Console.WriteLine("Users Found: " + musers.Count());

        }



        public async Task<string> ExistingCheck(user checkuser)
        {
            string id = "";
            IEnumerable<muser> matchedusers = null;
            JArray errors = new JArray();
            MailAddress testemail;
            bool dup = false;
            List<string> where = new List<string>();

            testemail = null;

            EmailCheck checkemail = new EmailCheck();

            if (checkemail.IsValidEmail(checkuser.email))
            {
                // if email is of correct format
                try
                {
                    testemail = new MailAddress(checkuser.email);
                }
                catch
                {
                    testemail = null;
                }
            }

            if (testemail == null || testemail.Address != checkuser.email)
            {
                // Email is invalid.
                dup = true;
                errors.Add(JObject.FromObject(new Dictionary<String, String>() { { "error", "Invalid Email Address" }, { "email", checkuser.email } }));
            }
            else
            {
                // Check to see if email already exists.
                where.Add("email.ToLower() == \"" + checkuser.email.ToLower() + "\"");
            }

            if (!String.IsNullOrWhiteSpace(checkuser.candidate_number))
            {
                // Check to see if candidate number already exists as idnumber.

                where.Add("idnumber == \"" + checkuser.candidate_number + "\"");
                where.Add("candidate_number == \"" + checkuser.candidate_number + "\"");
            }

            if (!String.IsNullOrWhiteSpace(checkuser.sapcustomernumber))
            {
                // Check to see if sap customer number already exists as idnumber.

                where.Add("idnumber == \"" + checkuser.sapcustomernumber + "\"");
                where.Add("sapcustomernumber == \"" + checkuser.sapcustomernumber + "\"");
            }

            if (!String.IsNullOrWhiteSpace(checkuser.sapsuppliernumber))
            {
                // Check to see if sap supplier number already exists as idnumber.

                where.Add("idnumber == \"" + checkuser.sapsuppliernumber + "\"");
                where.Add("sapsuppliernumber == \"" + checkuser.sapsuppliernumber + "\"");
            }

            string criteria = String.Join(" || ", where);
            matchedusers = musers.Where(criteria);

            if (matchedusers.Count() > 0)
            {
                // There are existing users that match the current user, so check to see if they're the same person.


                // Get distinct record numbers
                IEnumerable<string> candnos = matchedusers.Where(t => !String.IsNullOrWhiteSpace(t.candidate_number)).Select(s => s.candidate_number).Distinct();
                IEnumerable<string> sapcs = matchedusers.Where(t => !String.IsNullOrWhiteSpace(t.sapcustomernumber)).Select(s => s.sapcustomernumber).Distinct();
                IEnumerable<string> sapss = matchedusers.Where(t => !String.IsNullOrWhiteSpace(t.sapsuppliernumber)).Select(s => s.sapsuppliernumber).Distinct();

                int nocandnos = candnos.Count();
                int nosapcs = sapcs.Count();
                int nosapss = sapss.Count();

                if ((nocandnos == 0 || String.IsNullOrWhiteSpace(checkuser.candidate_number)) && (nosapcs == 0 || String.IsNullOrWhiteSpace(checkuser.sapcustomernumber)) && (nosapss == 0 || String.IsNullOrWhiteSpace(checkuser.sapsuppliernumber)))
                {
                    // no corresponding cfields, so must match by idnumber or email only.  If so treat as duplicate for manual checking.
                    dup = true;
                    errors.Add(JObject.FromObject(new Dictionary<String, String>() { { "error", "User with matching email address but no Candidate or SAP numbers to verify" } }));
                }

                if (nocandnos > 1 || (!String.IsNullOrWhiteSpace(checkuser.candidate_number) && nocandnos > 0 && candnos.First() != checkuser.candidate_number))
                {
                    // We have candidate number mismatches, so raise an error.
                    dup = true;
                    errors.Add(JObject.FromObject(new Dictionary<String, JToken>() { { "error", JToken.FromObject("Candidate Number mismatch(s)") }, { "New Candidate No", checkuser.candidate_number }, { "Existing Candidate Nos", JToken.FromObject(candnos.Distinct()) } }));
                }

                if (nosapcs > 1 || (!String.IsNullOrWhiteSpace(checkuser.sapcustomernumber) && nosapcs > 0 && sapcs.First() != checkuser.sapcustomernumber))
                {
                    // We have SAP Customer mismatches, so check candidate numbers.
                    IEnumerable<muser> sapcandnos = null;

                    if (!String.IsNullOrWhiteSpace(checkuser.candidate_number))
                    {
                        // We have a candidate number, so check if any of the SAP Customers don't correspond to this.
                        sapcandnos = matchedusers.Where(t => !String.IsNullOrWhiteSpace(t.sapcustomernumber) && t.candidate_number != checkuser.candidate_number);
                    }
                    if (String.IsNullOrWhiteSpace(checkuser.candidate_number) || sapcandnos.Count() > 0 )
                    {
                        dup = true;
                        errors.Add(JObject.FromObject(new Dictionary<String, JToken>() { { "error", JToken.FromObject("SAP Customer Number mismatch(s)") }, { "New SAP Customer", checkuser.sapcustomernumber }, { "Existing SAP Customers", JToken.FromObject(sapcs.Distinct()) } }));
                    }
                }

                if (nosapss > 1 || (!String.IsNullOrWhiteSpace(checkuser.sapsuppliernumber) && nosapss > 0 && sapss.First() != checkuser.sapsuppliernumber))
                {
                    // We have SAP Supplier mismatches, so check candidate numbers.
                    IEnumerable<muser> sapcandnos = null;

                    if (!String.IsNullOrWhiteSpace(checkuser.candidate_number))
                    {
                        // We have a candidate number, so check if any of the SAP Suppliers don't corresponde to this.
                        sapcandnos = matchedusers.Where(t => !String.IsNullOrWhiteSpace(t.sapsuppliernumber) && t.candidate_number != checkuser.candidate_number);
                    }
                    if (String.IsNullOrWhiteSpace(checkuser.candidate_number) || sapcandnos.Count() > 0)
                    {
                        dup = true;
                        errors.Add(JObject.FromObject(new Dictionary<String, JToken>() { { "error", JToken.FromObject("SAP Supplier Number mismatch(s)") }, { "New SAP Supplier", checkuser.sapsuppliernumber }, { "Existing SAP Suppliers", JToken.FromObject(sapss.Distinct()) } }));
                    }
                }

                IEnumerable<string> usernames = matchedusers.Select(s => s.username).Distinct();
                int nounames = usernames.Count();

                if (nounames == 0)
                {
                    // Something has gone wrong.  Report as duplicate for investigation;

                    errors.Add(JObject.FromObject(new Dictionary<string, string>() { { "error", "Match found, but couldn't retrieve username" } }));
                    dup = true;
                }
                else if (nounames == 1)
                {
                    // Exactly one matching username found.

                    if (dup == false)
                    {
                        // No other issues found.
                        if (matchedusers.First().id != null)
                        {
                            // Add as an existing user.
                            id = matchedusers.First().id;
                            existingusers.Add(checkuser, id);
                        }
                        else
                        {
                            // Can't get ID from query string, so return as duplicate for further investigation.
                            dup = true;
                            errors.Add(JObject.FromObject(new Dictionary<string, string>() { { "error", "Cannot retrieve ID of user" } }));
                        }
                    }
                }
                else
                {
                    // Multiple matching usernames found.  Return as duplicates for further investigation.

                    errors.Add(JObject.FromObject(new Dictionary<string, JToken>() { { "error", JToken.FromObject("Multiple matching users") }, { "users", JToken.FromObject(usernames) } }));
                    dup = true;
                }
            }
            else
            {
                // No matches, so treat as new user.
                newusers.Add(checkuser);
            }

            if (dup)
            {
                // We have duplicates, add to duplicates object.
                if (duplicates.ContainsKey(checkuser))
                {
                    JArray duperrs = JArray.FromObject(duplicates[checkuser]["errors"]);
                    duperrs.Merge(errors);
                    duplicates[checkuser]["errors"] = duperrs;
                }
                else
                {
                    JObject duprets = new JObject();
                    duprets["errors"] = errors;
                    JObject checkuserjson = JObject.FromObject(checkuser);
                    checkuserjson.Property("password").Remove();
                    duprets["user to add"] = checkuserjson;
                    if (matchedusers != null)
                    {
                        duprets["matching existing users"] = JArray.FromObject(matchedusers);
                    }
                    duplicates.Add(checkuser, duprets);
                }
                string usermatch = checkuser.firstname + " " + checkuser.middlename + " " + checkuser.lastname;
                List<string> userids = new List<string>();
                if (!string.IsNullOrWhiteSpace(checkuser.candidate_number))
                {
                    if (!userids.Contains(checkuser.candidate_number))
                    {
                        userids.Add(checkuser.candidate_number);
                    }
                }
                if (!string.IsNullOrWhiteSpace(checkuser.sapcustomernumber))
                {
                    if (!userids.Contains(checkuser.sapcustomernumber))
                    {
                        userids.Add(checkuser.sapcustomernumber);
                    }
                }
                if (!string.IsNullOrWhiteSpace(checkuser.sapsuppliernumber))
                {
                    if (!userids.Contains(checkuser.sapsuppliernumber))
                    {
                        userids.Add(checkuser.sapsuppliernumber);
                    }
                }
                string useridstring = String.Join("/", userids);
                if (!string.IsNullOrWhiteSpace(useridstring))
                {
                    usermatch += " (" + useridstring + ")";
                }
                Console.WriteLine();
                Console.WriteLine("Duplicate User Found: " + usermatch);
                Console.WriteLine(JArray.FromObject(errors).ToString());
                Console.Write("Checking Existing Enrolments");

                return null;
            }
            else
            {

                // Return the id if we were able to find a match, otherwise nothing if duplicate or new user.
                return id;
            }
        }
    }

    public static class GetUsers
    {
        // Get user as dataset from ODBC or SQL source.

        public static DataTable ODBC(string connectionstring, string query) 
        {
            // Setup connection using provided string.
            OdbcConnection connection = new OdbcConnection();
            connection.ConnectionString = connectionstring;

            DataTable data = new DataTable();

            short retries = 0;

            OdbcDataAdapter adapter;

            // Try to get query (3 attempts).
            while (true)
            {
                try
                {
                    adapter = new OdbcDataAdapter(query, connection);
                    // Pull data into dataset
                    adapter.Fill(data);
                    break;
                }
                catch(Exception)
                {
                    retries++;
                    Console.WriteLine("Connection error");
                    if (retries > 2) throw;
                    Console.WriteLine("Retry " + retries);
                }
            }

            return data;
        }

        public static DataTable SQL(string connectionstring, string query)
        {
            // Setup connection using provided string.
            SqlConnection connection = new SqlConnection(connectionstring);

            DataTable data = new DataTable();

            short retries = 0;

            SqlDataAdapter adapter;

            // Try to get query (3 attempts).
            while (true)
            {
                try
                {
                    adapter = new SqlDataAdapter(query, connection);
                    // Pull data into dataset
                    adapter.Fill(data);
                    break;
                }
                catch(Exception)
                {
                    retries++;
                    Console.WriteLine("Connection error");
                    if (retries > 2) throw;
                    Console.WriteLine("Retry " + retries);
                }
            }

            return data;
        }

        public static DataTable MoodleODBC(string query)
        {
            string username = Properties.moodlerest.Default.DBUser;
            string password = "";
            if (!string.IsNullOrWhiteSpace(Properties.moodlerest.Default.DBPass))
            {
                byte[] enc = System.Convert.FromBase64String(Properties.moodlerest.Default.DBPass);
                byte[] entropy = System.Convert.FromBase64String(Properties.moodlerest.Default.DBEntropy);
                byte[] unenc = ProtectedData.Unprotect(enc, entropy, DataProtectionScope.CurrentUser);
                password = Encoding.Unicode.GetString(unenc);
            }
            string server = Properties.moodlerest.Default.DBServer;
            string database = Properties.moodlerest.Default.Database;
            string port = Properties.moodlerest.Default.DBPort;

            // Setup connection using provided details.
            string connectionstring = "Driver={MySQL ODBC 5.3 Unicode Driver}; Server=" + server + "; Port=" + port + "; Database=" + database + "; Uid=" + username + "; Pwd=" + password;

            return ODBC(connectionstring, query);
        }

    }

    public static class ProcessData
    {
        public static List<Enrolment> Enrolments(DataTable data, string id, Dictionary<string,string> fieldmappings)
        {
            List<Enrolment> Enrols = new List<Enrolment>();
            Dictionary<string,user> users = new Dictionary <string,user>();
            Dictionary<string, Dictionary<string, bool>> cohorts = new Dictionary<string, Dictionary<string,bool>>();
            Dictionary<string, Dictionary<string, courseenrol>> courses = new Dictionary<string, Dictionary<string, courseenrol>>();

            foreach (DataRow row in data.Rows)
            {
                if (!users.ContainsKey(row[id].ToString()))
                {
                    // Add user to Dictionary if not already present
                    user newuser = new user();

                    foreach (PropertyInfo propertyinfo in newuser.GetType().GetProperties(BindingFlags.Public | BindingFlags.Instance))
                    {
                        // Check which user fields exist in data and transfer to user object.

                        if (fieldmappings.ContainsKey(propertyinfo.Name))
                        {
                            if (data.Columns.Contains(fieldmappings[propertyinfo.Name]))
                            {
                                if (propertyinfo.PropertyType == typeof(string))
                                {
                                    propertyinfo.SetValue(newuser, row[fieldmappings[propertyinfo.Name]].ToString());
                                }
                                else if (propertyinfo.PropertyType == typeof(bool))
                                {
                                    bool value = true; // default to true (generate password, force change, etc)
                                    bool.TryParse(row[fieldmappings[propertyinfo.Name]].ToString(), out value);
                                    propertyinfo.SetValue(newuser, value);
                                }
                            }
                        }
                        else if (data.Columns.Contains(propertyinfo.Name))
                        {
                            if (propertyinfo.PropertyType == typeof(string))
                            {
                                propertyinfo.SetValue(newuser, row[propertyinfo.Name].ToString());
                            }
                            else if (propertyinfo.PropertyType == typeof(bool))
                            {
                                bool value = true; // default to true (generate password, force change, etc)
                                bool.TryParse(row[propertyinfo.Name].ToString(), out value);
                                propertyinfo.SetValue(newuser, value);
                            }
                        }
                    }

                    users.Add(row[id].ToString(), newuser);
                }

                // Check for cohort enrolments
                string newcohort = "";
                bool newchdelete = false; // default to false (not deleted)

                if (fieldmappings.ContainsKey("cohort"))
                {
                    if(data.Columns.Contains(fieldmappings["cohort"]))
                    {
                        newcohort = row[fieldmappings["cohort"]].ToString();
                    }
                }
                else
                {
                    if(data.Columns.Contains("cohort"))
                    {
                        newcohort = row["cohort"].ToString();
                    }
                }

                if (fieldmappings.ContainsKey("chdelete"))
                {
                    if(data.Columns.Contains(fieldmappings["chdelete"]))
                    {
                        bool.TryParse(row[fieldmappings["chdelete"]].ToString(), out newchdelete);
                    }
                }
                else
                {
                    if(data.Columns.Contains("chdelete"))
                    {
                        bool.TryParse(row["chdelete"].ToString(), out newchdelete);
                    }
                }

                if(!string.IsNullOrWhiteSpace(newcohort))
                {
                    if (cohorts.ContainsKey(row[id].ToString()))
                    {
                        // User already has cohorts.

                        if (cohorts[row[id].ToString()].ContainsKey(newcohort))
                        {
                            // User already has a record for this cohort.
                            if (cohorts[row[id].ToString()][newcohort])
                            {
                                // Current record is marked as deleted, so replace with this one.
                                cohorts[row[id].ToString()][newcohort] = newchdelete;
                            }
                        }
                        else {
                            // User doesn't have current record, so add it.
                            cohorts[row[id].ToString()].Add(newcohort, newchdelete);
                        }
                    }
                    else
                    {
                        // user doesn't currently have any cohorts, so add them.

                        cohorts.Add(row[id].ToString(), new Dictionary<string, bool> { { newcohort, newchdelete } });
                    }
                }
                

                // Check for course enrolments.
                string newcourse = "";
                int newrole = 5;  // default role to 5 (student)
                bool newrdelete = false; // not deleted by default
                
                if (fieldmappings.ContainsKey("course")) 
                {
                    if(data.Columns.Contains(fieldmappings["course"]))
                    {
                        newcourse = row[fieldmappings["course"]].ToString();
                    }
                }
                else if(data.Columns.Contains("course")) {
                    newcourse = row["course"].ToString();
                }

                if (fieldmappings.ContainsKey("role")) 
                {
                    if(data.Columns.Contains(fieldmappings["role"]))
                    {
                        int.TryParse(row[fieldmappings["role"]].ToString(),out newrole);
                    }
                }
                else if(data.Columns.Contains("role"))
                {
                    int.TryParse(row["role"].ToString(), out newrole);
                }

                if (fieldmappings.ContainsKey("rdelete"))
                {
                    if(data.Columns.Contains(fieldmappings["rdelete"]))
                    {
                        bool.TryParse(row[fieldmappings["rdelete"]].ToString(), out newrdelete);
                    }
                }
                else if(data.Columns.Contains("rdelete"))
                {
                    bool.TryParse(row["rdelete"].ToString(), out newrdelete);
                }

                if(!string.IsNullOrWhiteSpace(newcourse))
                {
                    if (courses.ContainsKey(row[id].ToString()))
                    {
                        // user already has courses to add.

                        if (courses[row[id].ToString()].ContainsKey(newcourse))
                        {
                            // user already has roles in this course to add.

                            if (courses[row[id].ToString()][newcourse].roles.ContainsKey(newrole.ToString()))
                            {
                                // user already has this role in this course.
                                if (courses[row[id].ToString()][newcourse].roles[newrole.ToString()])
                                {
                                    // if current value is deleted, replace with this one, otherwise leave as is
                                    courses[row[id].ToString()][newcourse].roles[newrole.ToString()] = newrdelete;
                                }
                            }
                            else {
                                // user doesn't have this role, so add it
                                courses[row[id].ToString()][newcourse].roles.Add(newrole.ToString(), newrdelete);
                            }
                        }
                        else {
                            // user doesn't have this course, so add it.
                            courseenrol myenrol = new courseenrol();
 
                            myenrol.roles.Add(newrole.ToString(), newrdelete);

                            courses[row[id].ToString()].Add(newcourse, myenrol);
                        }
                    }
                    else {
                        // user doesn't currently have any courses, so add them.
                        courseenrol myenrol = new courseenrol();

                        myenrol.roles.Add(newrole.ToString(), newrdelete);

                        courses.Add(row[id].ToString(), new Dictionary<string, courseenrol> { { newcourse, myenrol } });
                    }
                }
            
            }

            List<string> newusers = users.Keys.ToList();
            

            // construct Enrolment object
            foreach( string myid in newusers) 
            {
                Enrols.Add(new Enrolment {user = users[myid]});

                if(courses.ContainsKey(myid)) {
                    Enrols.Last().courses = courses[myid];
                }

                if(cohorts.ContainsKey(myid)) {
                    Enrols.Last().cohorts = cohorts[myid];
                }
            }

            return Enrols;
        }
    }

    public class CreateUsers
    {
        // Class for creating new users.

        private string wsfunction { get; set; }
        public List<JToken> users { get; set; }
        public Dictionary<user, string> errors { get; set; }
        public OCWebRequest oc { get; set; }
        private Username usernamegen;


        public CreateUsers()
        {
            wsfunction = "local_extrauserlookups_create_users";
            users = new List<JToken>();
            errors = new Dictionary<user, string>();
            oc = new OCWebRequest();
            usernamegen = new Username();
        }

        public async Task AddUser( user user , string username = "")
        {
            // Add the user in the parameters to the list to add to moodle.
            
            // Generate new username based on first initial and lastname
            if (String.IsNullOrWhiteSpace(username))
            {
                try
                {
                    string usernameprefix = user.lastname;
                    if (!String.IsNullOrWhiteSpace(user.firstname))
                    {
                        // If the user has a firstname add first initial to start of username.
                        usernameprefix = user.firstname[0] + usernameprefix;
                        
                    }
                    username = await usernamegen.Generate(usernameprefix);
                }
                catch (HttpRequestException)
                {
                    // Unable to pull usernames so add to errors list
                    errors.Add(user, "Unable to generate username");
                    return;
                }
            }

            // Add core user details to list.
            users.Add(JObject.FromObject(new Dictionary<string,string> {{"username", username}, {"firstname", user.firstname}, {"lastname", user.lastname}, {"email", user.email}}));

            // Check if password has been specified and add if so.
            if(!string.IsNullOrWhiteSpace(user.password))
            {
                users.Last()["password"] = user.password;
            }
            else
            {
                // If no password, then pass random password string for now and generate a new password on the server instead.
                users.Last()["password"] = "&a1" + Membership.GeneratePassword(12, 3);
                user.generate_password = true;
            }

            // Check for other fields and add to object if necessary.
            if (!string.IsNullOrWhiteSpace(user.alternatename))
            {
                users.Last()["alternatename"] = user.alternatename;
            }
            if (!string.IsNullOrWhiteSpace(user.city))
            {
                users.Last()["city"] = user.city;
            }
            if(!string.IsNullOrWhiteSpace(user.country))
            {
                users.Last()["country"] = user.country;
            }
            if(!string.IsNullOrWhiteSpace(user.middlename))
            {
                users.Last()["middlename"] = user.middlename;
            }
            if(!string.IsNullOrWhiteSpace(user.institution))
            {
                users.Last()["institution"] = user.institution;
            }
            if(!string.IsNullOrWhiteSpace(user.address))
            {
                users.Last()["address"] = user.address;
            }
            if(!string.IsNullOrWhiteSpace(user.phone1))
            {
                users.Last()["phone1"] = user.phone1;
            }
            if(!string.IsNullOrWhiteSpace(user.phone2))
            {
                users.Last()["phone2"] = user.phone2;
            }
            if(!string.IsNullOrWhiteSpace(user.url))
            {
                users.Last()["url"] = user.url;
            }

            // Setup preferences;
            JArray preferences = new JArray();
            if (user.generate_password)
            {
                preferences.Add(JObject.FromObject(new Dictionary<string, string> { { "type", "create_password" }, { "value", "1" } }));
            }
            if (user.force_password_change)
            {
                preferences.Add(JObject.FromObject(new Dictionary<string, string> { { "type", "auth_forcepasswordchange" }, { "value", "1" } }));
            }
            if(preferences.HasValues) {
                users.Last()["preferences"] = preferences;
            }
            
            // Setup custom fields
            JArray customfields = new JArray();
            if( !String.IsNullOrWhiteSpace(user.candidate_number) )
            {
                customfields.Add(JObject.FromObject(new Dictionary<string, string> { { "type", "candno" }, { "value", user.candidate_number } }));
                users.Last()["idnumber"] = user.candidate_number;
            }
            if ( !String.IsNullOrWhiteSpace(user.sapcustomernumber) )
            {
                customfields.Add(JObject.FromObject(new Dictionary<string,string>{{"type", "SAPC"},{"value",user.sapcustomernumber}}));
                if( users.Last()["idnumber"] == null )
                {
                    users.Last()["idnumber"] = user.sapcustomernumber;
                }
            }
            if ( !String.IsNullOrWhiteSpace(user.sapsuppliernumber) )
            {
                customfields.Add(JObject.FromObject(new Dictionary<string, string> { { "type", "SAPS" }, { "value", user.sapsuppliernumber } }));
                if (users.Last()["idnumber"] == null)
                {
                    users.Last()["idnumber"] = user.sapsuppliernumber;
                }
            }
            if (customfields.HasValues)
            {
                users.Last()["customfields"] = customfields;
            }

            // Lookup timezone based on users location.
            users.Last()["timezone"] = await timezone.Lookup(user.city, user.country);

            Console.WriteLine("Adding user: " + users.Last()["username"] + " - " + users.Last()["firstname"] + " " + users.Last()["middlename"] + " " + users.Last()["lastname"] + " (" + users.Last()["email"] + ")");
        }

        public async Task AddUser( List<user> users, string uidfield = "") 
        {
            // Add a list of users to the object to upload.
            PropertyInfo uidprop = null;

            if (!String.IsNullOrWhiteSpace(uidfield))
            {
                uidprop = users[0].GetType().GetProperty(uidfield);
            }

            if (users.Count > 0)
            {
                foreach (user user in users)
                {
                    // For each user add them to the list.
                    if (uidprop != null)
                    {
                        await this.AddUser(user, uidprop.GetValue(user).ToString());
                    }
                    else
                    {
                        await this.AddUser(user);
                    }
                }
            }
        }

        public async Task Send()
        {
            // Send new users to moodle in batches.
            int i = 0;
            int batch = 50;
            while (i < users.Count)
            {
                int j = Math.Min(batch, users.Count-i);
                await oc.Send(new Dictionary<string, JToken>() { { "wsfunction", JToken.FromObject(wsfunction) }, { "users", JToken.FromObject(users.GetRange(i,j)) } });
                if (oc.response != null)
                {
                    JToken response = JToken.Parse(oc.response);
                    if (response != null && response.SelectToken("exception") != null)
                    {
                        // Error in update try adding individually
                        for (int k = i; k < i + j; k++)
                        {
                            await oc.Send(new Dictionary<string, JToken>() { { "wsfunction", JToken.FromObject(wsfunction) }, { "users", JToken.FromObject(users.GetRange(k, 1)) } });
                        }
                    }
                }
                i += batch;
            }
        }
    }

    public class UpdateUsers
    {
        // Class for updating existing users.

        private string wsfunction;
        public List<JToken> users { get; set; }
        public Dictionary<user, string> errors { get; set; }
        public OCWebRequest oc { get; set; }

        public UpdateUsers()
        {
            wsfunction = "local_extrauserlookups_update_users";
            users = new List<JToken>();
            errors = new Dictionary<user, string>();
            oc = new OCWebRequest();
        }

        public async Task AddUser( string id, user user, bool updtz = false )
        {
            // Add user's ID to upload object.
            users.Add(JObject.FromObject(new Dictionary<string,string> {{"id", id}}));


            // Add any non-empty values to the details to update.
            if(!string.IsNullOrWhiteSpace(user.password))
            {
                users.Last()["password"] = user.password;
            }
            if(!string.IsNullOrWhiteSpace(user.firstname))
            {
                users.Last()["firstname"] = user.firstname;
            }
            if(!string.IsNullOrWhiteSpace(user.lastname))
            {
                users.Last()["lastname"] = user.lastname;
            }
            if(!string.IsNullOrWhiteSpace(user.email))
            {
                users.Last()["email"] = user.email;
            }
            if (!string.IsNullOrWhiteSpace(user.alternatename))
            {
                users.Last()["alternatename"] = user.alternatename;
            }
            if (!string.IsNullOrWhiteSpace(user.city))
            {
                users.Last()["city"] = user.city;
            }
            if(!string.IsNullOrWhiteSpace(user.country))
            {
                users.Last()["country"] = user.country;
            }
            if(!string.IsNullOrWhiteSpace(user.middlename))
            {
                users.Last()["middlename"] = user.middlename;
            } 
            if (!string.IsNullOrWhiteSpace(user.institution))
            {
                users.Last()["institution"] = user.institution;
            }
            if (!string.IsNullOrWhiteSpace(user.address))
            {
                users.Last()["address"] = user.address;
            }
            if (!string.IsNullOrWhiteSpace(user.phone1))
            {
                users.Last()["phone1"] = user.phone1;
            }
            if (!string.IsNullOrWhiteSpace(user.phone2))
            {
                users.Last()["phone2"] = user.phone2;
            }
            if (!string.IsNullOrWhiteSpace(user.url))
            {
                users.Last()["url"] = user.url;
            }

            // Setup preferences;
            JArray preferences = new JArray();
            if (user.generate_password)
            {
                preferences.Add(JObject.FromObject(new Dictionary<string, string> { { "type", "create_password" }, { "value", "1" } }));
            }
            if (user.force_password_change)
            {
                preferences.Add(JObject.FromObject(new Dictionary<string, string> { { "type", "auth_forcepasswordchange" }, { "value", "1" } }));
            }
            if(preferences.HasValues) {
                users.Last()["preferences"] = preferences;
            }
            
            // Setup custom fields
            JArray customfields = new JArray();
            if( !String.IsNullOrWhiteSpace(user.candidate_number) )
            {
                customfields.Add(JObject.FromObject(new Dictionary<string, string> { { "type", "candno" }, { "value", user.candidate_number } }));
                users.Last()["idnumber"] = user.candidate_number;
            }
            if ( !String.IsNullOrWhiteSpace(user.sapcustomernumber) )
            {
                customfields.Add(JObject.FromObject(new Dictionary<string,string>{{"type", "SAPC"},{"value",user.sapcustomernumber}}));
                if( users.Last()["idnumber"] == null )
                {
                    users.Last()["idnumber"] = user.sapcustomernumber;
                }
            }
            if ( !String.IsNullOrWhiteSpace(user.sapsuppliernumber) )
            {
                customfields.Add(JObject.FromObject(new Dictionary<string, string> { { "type", "SAPS" }, { "value", user.sapsuppliernumber } }));
                if (users.Last()["idnumber"] == null)
                {
                    users.Last()["idnumber"] = user.sapsuppliernumber;
                }
            }
            if (customfields.HasValues)
            {
                users.Last()["customfields"] = customfields;
            }
            
            // Lookup new timezone only if requested.
            if (updtz) {
                users.Last()["timezone"] = await timezone.Lookup(user.city, user.country);
            }
            Console.WriteLine("Updating user: " + users.Last()["username"] + " - " + users.Last()["firstname"] + " " + users.Last()["middlename"] + " " + users.Last()["lastname"] + " (" + users.Last()["email"] + ")");
        }

        public async Task AddUser( Dictionary<string, user> users, bool updtz = false) 
        {
            // Add multiple users to the updates to upload.

            foreach( KeyValuePair<string, user> userpair in users )
            {
                // Add each user one at a time.
                await this.AddUser(userpair.Key, userpair.Value, updtz);
            };
        }

        public async Task Send()
        {
            // Send updates to moodle in batches.
            int i = 0;
            int batch = 50;
            while (i < users.Count)
            {
                int j = Math.Min(batch, users.Count-i);
                await oc.Send(new Dictionary<string, JToken>() { { "wsfunction", JToken.FromObject(wsfunction) }, { "users", JToken.FromObject(users.GetRange(i,j)) } });
                if (oc.response != null)
                {
                    JToken response = JToken.Parse(oc.response);
                    if (response != null && response.SelectToken("exception") != null)
                    {
                        // Error in update try updating individually
                        for(int k = i; k < i+j ; k++)
                        {
                            await oc.Send(new Dictionary<string, JToken>() { { "wsfunction", JToken.FromObject(wsfunction) }, { "users", JToken.FromObject(users.GetRange(k, 1)) } });
                        }
                    }
                }
                i += batch;
            }
        }
    }

    public class courseenrol
    {
        // Object for holding course enrolment data.
        public Dictionary<string,bool> roles { get; set; }

        public courseenrol()
        {
            roles = new Dictionary<string, bool>();
        }
    }

    public class Enrolment
    {
        // Object for holding all enrolment data for processing.
        public user user {get; set;}
        public Dictionary<string,courseenrol> courses {get; set;}
        public Dictionary<string,bool> cohorts {get; set;}

        public Enrolment()
        {
            courses = new Dictionary<string, courseenrol>();
            cohorts = new Dictionary<string, bool>();
        }
    }

    public class newcourseenrol
    {
        // Object for holding course enrolments during processing.
        public string userid { get; set; }
        public string courseid { get; set; }
        public string roleid { get; set; }
        public string suspend { get; set; }
    }

    public class newroles
    {
        // Object for holding role assignments during processing.
        public string userid { get; set; }
        public string roleid { get; set; }
        public string instanceid { get; set; }
        public string contextlevel { get; set; }

        public newroles()
        {
            contextlevel = "course";
        }
    }

    public class newcohortenrol
    {
        // Object for holding cohort enrolments during processing.
        public Dictionary<string, string> cohorttype { get; set; }
        public Dictionary<string, string> usertype { get; set; }

        public newcohortenrol()
        {
            cohorttype = new Dictionary<string,string>();
            usertype = new Dictionary<string, string>();

            cohorttype.Add("type", "id");
            usertype.Add("type", "id");
        }
    }


    public class EnrolUsers
    {
        // Class for enrolling users onto courses and cohorts.
        private string fngetcourse;
        private string fngetcohorts;
        private string fngetenrolments;
        private string fnsetenrolments;
        private string fngetchmembers;
        private string fnsetchmembers;
        private string fnrmchmembers;
        private string fnsetroles;
        private string fnrmroles;
        private CheckForUser usercheck;
        public OCWebRequest oc { get; set; }

        public Dictionary<user,List<newcourseenrol>> newusercourses {get; set;}
        public Dictionary<user, List<newcohortenrol>> newusercohorts { get; set; }
        public List<newcourseenrol> enrols {get; set;}
        public List<newcohortenrol> cenrols { get; set; }
        public List<Dictionary<string,string>> cremoves { get; set; }
        public List<newroles> addroles {get; set;} 
        public List<newroles> rmroles {get; set;}
        public List<user> newusers { get; set; }
        public Dictionary<string, user> upduser { get; set; }
        private Dictionary<string, string> courses;
        private Dictionary<string, string> cohorts;
        private Dictionary<string, DataTable> courseenrolments;
        private Dictionary<string, DataTable> cohortmembers;

        private List<Dictionary<string, string>> enrolgetopts;

        private bool error;
        List<Exception> exceptions;

        public EnrolUsers( )
        {
            usercheck = new CheckForUser();
            oc = new OCWebRequest();

            // Set required functions
            fngetcourse = "core_course_get_courses";

            fngetcohorts = "local_extrauserlookups_get_cohorts";

            fngetenrolments = "core_enrol_get_enrolled_users"; 
            fnsetenrolments = "enrol_manual_enrol_users";
            fngetchmembers = "core_cohort_get_cohort_members";
            fnsetchmembers = "core_cohort_add_cohort_members";
            fnrmchmembers = "core_cohort_delete_cohort_members";

            fnsetroles = "core_role_assign_roles";
            fnrmroles = "core_role_unassign_roles";

            enrols = new List<newcourseenrol>();
            newusercourses = new Dictionary<user,List<newcourseenrol>>();
            newusercohorts = new Dictionary<user, List<newcohortenrol>>();
            newusers = new List<user>();
            cenrols = new List<newcohortenrol>();
            cremoves = new List<Dictionary<string,string>>();
            addroles = new List<newroles>();
            rmroles = new List<newroles>();

            upduser = new Dictionary<string,user>();

            courses = new Dictionary<string, string>();
            cohorts = new Dictionary<string, string>();
            courseenrolments = new Dictionary<string, DataTable>();
            cohortmembers = new Dictionary<string, DataTable>();

            enrolgetopts = new List<Dictionary<string, string>>() { new Dictionary<string, string>() { { "name", "onlyactive" }, { "value", "1" } } };

            error = false;
            exceptions = new List<Exception>();
        }

        private async Task<bool> SendEnrolment ()
        {
            // Unassign old roles.
            if (rmroles.Count > 0)
            {
                Console.WriteLine("Unassigning removed roles");
                try
                {
                    await oc.Send(new Dictionary<string, JToken> { { "wsfunction", JToken.FromObject(fnrmroles) }, { "unassignments", JToken.FromObject(rmroles) } });
                }
                catch (HttpRequestException e)
                {
                    // if there's an issue with the HTTP Request, record the error and continue with the next part of the upload. 
                    error = true;
                    exceptions.Add(e);
                    Console.WriteLine("Communications Error");
                }
                catch (WebException e)
                {
                    // if there's an issue with the HTTP Request, record the error and continue with the next part of the upload. 
                    error = true;
                    exceptions.Add(e);
                    Console.WriteLine("Communications Error");
                }
            }

            // Update course enrolments
            if (enrols.Count > 0)
            {
                Console.WriteLine("Updating course enrolments");
                try
                {
                    await oc.Send(new Dictionary<string, JToken> { { "wsfunction", JToken.FromObject(fnsetenrolments) }, { "enrolments", JToken.FromObject(enrols) } });
                }
                catch (HttpRequestException e)
                {
                    // if there's an issue with the HTTP Request, record the error and continue with the next part of the upload. 
                    error = true;
                    exceptions.Add(e);
                    Console.WriteLine("Communications Error");
                }
                catch (WebException e)
                {
                    // if there's an issue with the HTTP Request, record the error and continue with the next part of the upload. 
                    error = true;
                    exceptions.Add(e);
                    Console.WriteLine("Communications Error");
                }
            }

            // Assign new roles.
            if (addroles.Count > 0)
            {
                Console.WriteLine("Assigning new roles");
                try
                {
                    await oc.Send(new Dictionary<string, JToken> { { "wsfunction", JToken.FromObject(fnsetroles) }, { "assignments", JToken.FromObject(addroles) } });
                }
                catch (HttpRequestException e)
                {
                    // if there's an issue with the HTTP Request, record the error and continue with the next part of the upload. 
                    error = true;
                    exceptions.Add(e);
                    Console.WriteLine("Communications Error");
                }
                catch (WebException e)
                {
                    // if there's an issue with the HTTP Request, record the error and continue with the next part of the upload. 
                    error = true;
                    exceptions.Add(e);
                    Console.WriteLine("Communications Error");
                }
            }

            // Remove old cohort enrolments
            if (cremoves.Count > 0)
            {
                Console.WriteLine("Removing stale cohorts enrolments");
                try
                {
                    await oc.Send(new Dictionary<string, JToken> { { "wsfunction", JToken.FromObject(fnrmchmembers) }, { "members", JToken.FromObject(cremoves) } });
                }
                catch (HttpRequestException e)
                {
                    // if there's an issue with the HTTP Request, record the error and continue with the next part of the upload. 
                    error = true;
                    exceptions.Add(e);
                    Console.WriteLine("Communications Error");
                }
                catch (WebException e)
                {
                    // if there's an issue with the HTTP Request, record the error and continue with the next part of the upload. 
                    error = true;
                    exceptions.Add(e);
                    Console.WriteLine("Communications Error");
                }
            }

            // Add new cohort enrolments
            if (cenrols.Count > 0)
            {
                Console.WriteLine("Adding new cohort enrolments");
                try
                {
                    await oc.Send(new Dictionary<string, JToken> { { "wsfunction", JToken.FromObject(fnsetchmembers) }, { "members", JToken.FromObject(cenrols) } });
                }
                catch (HttpRequestException e)
                {
                    // if there's an issue with the HTTP Request, record the error and continue with the next part of the upload. 
                    error = true;
                    exceptions.Add(e);
                    Console.WriteLine("Communications Error");
                }
                catch (WebException e)
                {
                    // if there's an issue with the HTTP Request, record the error and continue with the next part of the upload. 
                    error = true;
                    exceptions.Add(e);
                    Console.WriteLine("Communications Error");
                }
            }

            return true;
        }

        private async Task<bool> Unenrolnoroll()
        {
            List<newcourseenrol> unenrols = new List<newcourseenrol>();
            List<newroles> exroles = new List<newroles>();

            Console.WriteLine("Getting Enrolments without Roles");
            DataTable dtcourses = GetUsers.MoodleODBC("SELECT Course.userid, Course.courseid FROM (SELECT mdl_user_enrolments.userid, mdl_enrol.courseid FROM mdl_course INNER JOIN (mdl_user_enrolments INNER JOIN mdl_enrol ON mdl_user_enrolments.enrolid = mdl_enrol.id) ON mdl_course.id = mdl_enrol.courseid WHERE (((mdl_user_enrolments.status)='0'))) Course LEFT JOIN (SELECT mdl_role_assignments.roleid, mdl_role_assignments.userid, mdl_context.instanceid FROM mdl_context INNER JOIN mdl_role_assignments ON mdl_context.id = mdl_role_assignments.contextid WHERE (((mdl_context.contextlevel)='50'))) Roles ON (Course.courseid = Roles.instanceid) AND (Course.userid = Roles.userid) WHERE (((Roles.roleid) Is Null))");

            foreach (DataRow row in dtcourses.AsEnumerable())
            {
                unenrols.Add(new newcourseenrol() { userid = row["userid"].ToString(), courseid = row["courseid"].ToString(), suspend = "1", roleid = "5" });
                exroles.Add(new newroles() { userid = row["userid"].ToString(), instanceid = row["courseid"].ToString(), roleid = "5" });
            }

            if(unenrols.Count > 0)
            {
                // Suspend user enrolments.
                try
                {
                    await oc.Send(new Dictionary<string, JToken> { { "wsfunction", JToken.FromObject(fnsetenrolments) }, { "enrolments", JToken.FromObject(unenrols) } });
                }
                catch (HttpRequestException e)
                {
                    // if there's an issue with the HTTP Request, record the error and continue with the next part of the upload. 
                    error = true;
                    exceptions.Add(e);
                    Console.WriteLine("Communications Error");
                }
                catch (WebException e)
                {
                    // if there's an issue with the HTTP Request, record the error and continue with the next part of the upload. 
                    error = true;
                    exceptions.Add(e);
                    Console.WriteLine("Communications Error");
                }

            }

            if(exroles.Count > 0)
            {
                // Remove extra roles just created.
                try
                {
                    await oc.Send(new Dictionary<string, JToken> { { "wsfunction", JToken.FromObject(fnrmroles) }, { "unassignments", JToken.FromObject(exroles) } });
                }
                catch (HttpRequestException e)
                {
                    // if there's an issue with the HTTP Request, record the error and continue with the next part of the upload. 
                    error = true;
                    exceptions.Add(e);
                    Console.WriteLine("Communications Error");
                }
                catch (WebException e)
                {
                    // if there's an issue with the HTTP Request, record the error and continue with the next part of the upload. 
                    error = true;
                    exceptions.Add(e);
                    Console.WriteLine("Communications Error");
                }
            }

            return true;
        }

        private async Task<bool> Check (Enrolment enrolment, Boolean checkallcohorts = false)
        {
            // Check which users need creating and which course and cohort enrolments to upload
            bool courseexist = false;
            bool cohortexist = false;
            string id = "";

            if (enrolment.courses.Count > 0)
            {
                // Only run if we have course enrolments to check.

                if (courses.Count == 0)
                {
                    // Get course codes from Moodle if we haven't already.

                    Console.WriteLine();
                    Console.WriteLine("Getting Moodle Courses");
                    DataTable dtcourses = GetUsers.MoodleODBC("SELECT shortname, id FROM mdl_course");
                    foreach (DataRow course in dtcourses.AsEnumerable())
                    {
                        courses.Add(course["shortname"].ToString(), course["id"].ToString());
                    }
                    Console.WriteLine("Courses Found: " + courses.Count);
                    Console.Write("Checking Existing Enrolments");
                }

                foreach (string course in enrolment.courses.Keys)
                {
                    // Check to see if any of the users courses are relevant for upload.
                    if (courses.ContainsKey(course))
                    {
                        courseexist = true;
                        break;
                    }
                }
            }

            if (courseexist)
            {
                //  There are relevant courses, so check to see if the user exists.
                id = await usercheck.ExistingCheck(enrolment.user);

                if (String.IsNullOrWhiteSpace(id) && !usercheck.duplicates.ContainsKey(enrolment.user))
                {
                    if (usercheck.newusers.Contains(enrolment.user))
                    {
                        // user needs creating, so put relevant enrolments in a holding object for now until this has been done.
                        List<newcourseenrol> userscourses = new List<newcourseenrol>();
                        foreach (string course in enrolment.courses.Keys)
                        {
                            if (courses.ContainsKey(course))
                            {
                                foreach (string role in enrolment.courses[course].roles.Keys)
                                {
                                    if (enrolment.courses[course].roles[role] != true)
                                    { // ignore any suspended enrolments.
                                        userscourses.Add(new newcourseenrol() { courseid = courses[course], roleid = role });
                                    }
                                }
                            }
                        }

                        if (userscourses.Count > 0)
                        {
                            // Only add user if there are enrolments to add.

                            if(!newusers.Contains(enrolment.user))
                            {
                                newusers.Add(enrolment.user);
                            }

                            if (newusercourses.ContainsKey(enrolment.user))
                            {
                                newusercourses[enrolment.user].AddRange(userscourses);
                            }
                            else
                            {
                                newusercourses.Add(enrolment.user, userscourses);
                            }
                        }
                    }
                }
                else if(!String.IsNullOrWhiteSpace(id))
                {
                    // user already exists, so we just need to add the enrolments to the list to upload.
                    foreach (string course in enrolment.courses.Keys)
                    {
                        if (courses.ContainsKey(course))
                        {
                            if (!courseenrolments.ContainsKey(course))
                            {
                                // if we haven't already looked up the enrolments for the course, do so.
                                Console.WriteLine();
                                Console.WriteLine("Getting Course Enrolments from Moodle for course " + course);
                                DataTable dtenrols = GetUsers.MoodleODBC("SELECT ra.userid, ra.roleid, e.courseid FROM mdl_role_assignments ra INNER JOIN mdl_context c ON ra.contextid = c.id INNER JOIN mdl_enrol e ON c.instanceid = e.courseid INNER JOIN mdl_user_enrolments ue ON ra.userid = ue.userid AND e.id = ue.enrolid WHERE ue.status = 0 AND e.enrol = \"manual\" AND e.courseid = " + courses[course]);
                                Console.WriteLine("Enrolments Found: " + dtenrols.Rows.Count);
                                Console.Write("Checking Existing Enrolments");

                                courseenrolments.Add(course, dtenrols);
                            }

                            // select user if they exist
                            IEnumerable<string> userrole = courseenrolments[course].AsEnumerable().Where(x => x["userid"].ToString() == id).Select(y => y["roleid"].ToString()); 

                            if (userrole.Count() == 0)
                            {
                                // user not enrolled in course or doesn't have any roles, so add enrolments
                                foreach (string role in enrolment.courses[course].roles.Keys)
                                {
                                    if (enrolment.courses[course].roles[role] == false)
                                    { // ignore any suspended enrolments.
                                        enrols.Add(new newcourseenrol() { userid = id, courseid = courses[course], roleid = role });

                                        if (!upduser.ContainsKey(id))
                                        {
                                            user thisuser = enrolment.user;
                                            thisuser.force_password_change = false;
                                            thisuser.generate_password = false;
                                            upduser.Add(id, thisuser);
                                        }
                                    }
                                }
                            }
                            else
                            {
                                // user is on course with roles, so check if these need updating.
                                foreach (string role in enrolment.courses[course].roles.Keys)
                                {
                                    if (enrolment.courses[course].roles[role] == false) // not suspended
                                    {
                                        if (!userrole.Contains(role))
                                        {
                                            // user doesn't currently have role
                                            addroles.Add(new newroles() { userid = id, instanceid = courses[course], roleid = role });

                                            if (!upduser.ContainsKey(id))
                                            {
                                                user thisuser = enrolment.user;
                                                thisuser.force_password_change = false;
                                                thisuser.generate_password = false;
                                                upduser.Add(id, thisuser);
                                            }
                                        }
                                    }
                                    else // suspended
                                    {
                                        if (enrolment.courses[course].roles[role] == true) // suspended
                                        {
                                            if (userrole.Contains(role))
                                            {
                                                //user currently has the role
                                                rmroles.Add(new newroles() { userid = id, instanceid = courses[course], roleid = role });
                                            }
                                        }
                                    }
                                }
                            }
                        }
                    }

                }
            }

            if (courseexist || checkallcohorts)
            {
                if (enrolment.cohorts.Count > 0)
                {
                    // Only run if we have cohort enrolments to check.

                    if (cohorts.Count == 0)
                    {
                        // Get cohorts from moodle if we haven't already.
                        Console.WriteLine();
                        Console.WriteLine("Getting Moodle Cohorts");
                        DataTable dtcohorts = GetUsers.MoodleODBC("SELECT id, idnumber FROM mdl_cohort WHERE contextid=1");
                        foreach (DataRow cohort in dtcohorts.AsEnumerable())
                        {
                            cohorts.Add(cohort["idnumber"].ToString(), cohort["id"].ToString());
                        }
                        Console.WriteLine("Cohorts Found: " + cohorts.Count);
                        Console.Write("Checking Existing Enrolments");
                    }

                    foreach (string cohort in enrolment.cohorts.Keys)
                    {
                        // Check to see if any of the users courses are relevant for upload.
                        if (cohorts.ContainsKey(cohort))
                        {
                            cohortexist = true;
                            break;
                        }
                    }
                }

                if (cohortexist)
                {
                    // There are relevant cohorts, so see if we have already checked if this user exists, if not run check
                    if (!courseexist)
                    {
                        id = await usercheck.ExistingCheck(enrolment.user);
                    }

                    if (String.IsNullOrWhiteSpace(id) && !usercheck.duplicates.ContainsKey(enrolment.user))
                    {
                        if (usercheck.newusers.Contains(enrolment.user))
                        {
                            // user needs creating, so put relevant enrolments in a holding object for now until this has been done.
                            List<newcohortenrol> userscohorts = new List<newcohortenrol>();
                            foreach (string cohort in enrolment.cohorts.Keys)
                            {
                                if (cohorts.ContainsKey(cohort) && enrolment.cohorts[cohort] == false)
                                {
                                    userscohorts.Add(new newcohortenrol());
                                    userscohorts.Last().cohorttype.Add("value", cohorts[cohort]);
                                }
                            }

                            if (userscohorts.Count > 0)
                            {
                                if (!newusers.Contains(enrolment.user))
                                {
                                    newusers.Add(enrolment.user);
                                }

                                if (newusercohorts.ContainsKey(enrolment.user))
                                {
                                    newusercohorts[enrolment.user].AddRange(userscohorts);
                                }
                                else
                                {
                                    newusercohorts.Add(enrolment.user, userscohorts);
                                }
                            }
                        }
                    }
                    else if(!String.IsNullOrWhiteSpace(id))
                    {
                        // user already exists, so check cohort membership.
                        foreach (string cohort in enrolment.cohorts.Keys)
                        {
                            if (cohorts.ContainsKey(cohort))
                            {
                                if (!cohortmembers.ContainsKey(cohort))
                                {
                                    // if we haven't already looked up the membership for the cohort, do so.
                                    Console.WriteLine();
                                    Console.WriteLine("Getting Cohort Enrolments from Moodle for cohort " + cohort);
                                    DataTable cmembers = GetUsers.MoodleODBC("SELECT m.userid FROM mdl_cohort_members m WHERE m.cohortid = " + cohorts[cohort]);
                                    Console.WriteLine("Enrolments Found: " + cmembers.Rows.Count);
                                    Console.Write("Checking Existing Enrolments");
                                    cohortmembers.Add(cohort, cmembers);
                                }

                                // select user if they exist
                                IEnumerable<DataRow> usermembership = cohortmembers[cohort].AsEnumerable().Where(x => x["userid"].ToString() == id);
                                int membcount = usermembership.Count();

                                if (membcount == 0 && enrolment.cohorts[cohort] == false)
                                {
                                    // Object null and cohort enrolment not deleted, so user not in cohort and needs adding.
                                    newcohortenrol newcenrol = new newcohortenrol();
                                    newcenrol.cohorttype.Add("value", cohorts[cohort]);
                                    newcenrol.usertype.Add("value", id);

                                    cenrols.Add(newcenrol);

                                    if (!upduser.ContainsKey(id))
                                    {
                                        user thisuser = enrolment.user;
                                        thisuser.force_password_change = false;
                                        thisuser.generate_password = false;
                                        upduser.Add(id, thisuser);
                                    }
                                }
                                else if (membcount > 0 && enrolment.cohorts[cohort] == true)
                                {
                                    // Object not null and cohort enrolment deleted, so user is in cohort and needs removing.
                                    cremoves.Add(new Dictionary<string,string>{{"cohortid", cohorts[cohort].ToString()},{"userid", id.ToString()}});
                                }
                            }
                        }
                    }
                }
            }

            return true;
        }

        public async Task<bool> Sync (List<Enrolment> enrolments, Boolean addnewusers=false, Boolean includecohortonlyenrols=false, String uidfield = "")
        {
            Console.Write("Checking Existing Enrolments");
            foreach (Enrolment enrolment in enrolments)
            {
                // Check which enrolments need creating.
                try
                {
                    await this.Check(enrolment, includecohortonlyenrols);
                    Console.Write(".");
                }
                catch (HttpRequestException e)
                {
                    // Communication error during check, so record error and skip to next enrolment
                    error = true;
                    exceptions.Add(e);
                    Console.WriteLine("Communications Error");
                }
                catch (TaskCanceledException e)
                {
                    // Comms error while adding new users, so record error and continue with existing users only
                    error = true;
                    exceptions.Add(e);
                    Console.WriteLine("Communications Timeout");
                }
            }
            Console.Write("Done"+System.Environment.NewLine);

            Console.Write("Creating Tickets for Duplicates");
            foreach (user dupuser in usercheck.duplicates.Keys)
            {
                // For each duplicate user, create a new ticket with details of the errors that occured.
                string subject = "Duplicate User ";
                if(!string.IsNullOrWhiteSpace(dupuser.firstname))
                {
                    subject += dupuser.firstname + " ";
                }
                if(!string.IsNullOrWhiteSpace(dupuser.lastname))
                {
                    subject += dupuser.lastname + " ";
                }
                subject += "(";
                bool comma = false;
                if(!string.IsNullOrWhiteSpace(dupuser.candidate_number))
                {
                    subject += dupuser.candidate_number;
                    comma = true;
                }
                if(!string.IsNullOrWhiteSpace(dupuser.sapcustomernumber))
                {
                    if(comma)
                    {
                        subject += ",";
                    }
                    subject += dupuser.sapcustomernumber;
                    comma = true;
                }
                if(!string.IsNullOrWhiteSpace(dupuser.sapsuppliernumber))
                {
                    if(comma)
                    {
                        subject += ",";
                    }
                    subject += dupuser.sapsuppliernumber;
                }
                subject += ")";

                string message = subject + System.Environment.NewLine + System.Environment.NewLine;

                message += "Errors:" + System.Environment.NewLine + usercheck.duplicates[dupuser].ToString();

                CreateNewTicket otrs = new CreateNewTicket();

                try
                {
                    await otrs.CreateTicketAsync(subject, message, Customer: otrssettings.customer, Queue: otrssettings.dupqueue);
                }
                catch (HttpException)
                {
                    //unable to create ticket so print to screen
                    Console.WriteLine("Unable to create ticket");
                    Console.WriteLine("Subject: " + subject);
                    Console.WriteLine("Message:");
                    Console.Write(message);
                    Console.WriteLine("");
                }
                catch (TaskCanceledException)
                {
                    //unable to create ticket so print to screen
                    Console.WriteLine("Unable to create ticket");
                    Console.WriteLine("Subject: " + subject);
                    Console.WriteLine("Message:");
                    Console.Write(message);
                    Console.WriteLine("");
                }

                Console.Write(".");
            }
            Console.Write("Done" + System.Environment.NewLine);

            if (addnewusers)
            {
                // Create new users.
                try
                {
                    CreateUsers addusers = new CreateUsers();

                    Console.WriteLine("Checking Enrolments for New Users");
                    if (newusers.Count > 0)
                    {
                        await addusers.AddUser(newusers, uidfield);
                        await addusers.Send();
                        Console.WriteLine("New Users: " + newusers.Count);

                        CheckForUser addusercheck = new CheckForUser();

                        Console.WriteLine("Processing Course & Cohort Enrolments for New Users");
                        // Add courses for new users
                        foreach (user newuser in newusers)
                        {
                            // Get generated id for each new user
                            string id = await addusercheck.ExistingCheck(newuser);
                            if (!String.IsNullOrWhiteSpace(id))
                            {
                                if (newusercourses.ContainsKey(newuser))
                                {
                                    // If there are courses for that user add them.
                                    foreach (newcourseenrol course in newusercourses[newuser])
                                    {
                                        enrols.Add(course);
                                        enrols.Last().userid = id;
                                    }
                                }

                                if (newusercohorts.ContainsKey(newuser))
                                {
                                    // If there are cohorts for that user add them.
                                    foreach (newcohortenrol cohort in newusercohorts[newuser])
                                    {
                                        cenrols.Add(cohort);
                                        cenrols.Last().usertype.Add("value", id);
                                    }
                                }
                            }
                            Console.Write(".");
                        }
                    }
                }
                catch (HttpRequestException e)
                {
                    // Comms error while adding new users, so record error and continue with existing users only
                    error = true;
                    exceptions.Add(e);
                    Console.WriteLine("Communications Error");
                }
                catch (WebException e)
                {
                    // Comms error while adding new users, so record error and continue with existing users only
                    error = true;
                    exceptions.Add(e);
                    Console.WriteLine("Communications Error");
                }
                catch (TaskCanceledException e)
                {
                    // Comms error while adding new users, so record error and continue with existing users only
                    error = true;
                    exceptions.Add(e);
                    Console.WriteLine("Communications Timeout");
                }
            }
            else
            {
                Console.WriteLine("New Users Needing Creation: " + newusers.Count);
            }

            // Update existing users
            try
            {
                if (upduser.Count > 0)
                {
                    UpdateUsers update = new UpdateUsers();
                    await update.AddUser(upduser);
                    await update.Send();
                }
            }
            catch(HttpRequestException e)
            {
                // Comms error while updating users, so record error and continue with enrolments
                error = true;
                exceptions.Add(e);
                Console.WriteLine("Communications Error");
            }
            catch (WebException e)
            {
                // Comms error while updating users, so record error and continue with enrolments
                error = true;
                exceptions.Add(e);
                Console.WriteLine("Communications Error");
            }
            catch (TaskCanceledException e)
            {
                // Comms error while adding new users, so record error and continue with existing users only
                error = true;
                exceptions.Add(e);
                Console.WriteLine("Communications Timeout");
            }
            

            // Send enrolment updates.
            try
            {
                await this.SendEnrolment();
            }
            catch (HttpRequestException e)
            {
                // Comms errors should be picked up within SendEnrolment() itself, but if any make it here record them and continue on to next part of upload.
                error = true;
                exceptions.Add(e);
                Console.WriteLine("Communications Error");
            }
            catch (WebException e)
            {
                // Comms error while updating users, so record error and continue with enrolments
                error = true;
                exceptions.Add(e);
                Console.WriteLine("Communications Error");
            }
            catch (TaskCanceledException e)
            {
                // Comms error while adding new users, so record error and continue with existing users only
                error = true;
                exceptions.Add(e);
                Console.WriteLine("Communications Timeout");
            }

            // Unenrol users without roles.
            try
            {
                Console.WriteLine("Tidying Up");
                await this.Unenrolnoroll();
            }
            catch (HttpRequestException e)
            {
                // Comms error while unenrolling, so record error and continue to report.
                error = true;
                exceptions.Add(e);
                Console.WriteLine("Communications Error");
            }
            catch (WebException e)
            {
                // Comms error while updating users, so record error and continue with enrolments
                error = true;
                exceptions.Add(e);
                Console.WriteLine("Communications Error");
            }
            catch (TaskCanceledException e)
            {
                // Comms error while adding new users, so record error and continue with existing users only
                error = true;
                exceptions.Add(e);
                Console.WriteLine("Communications Timeout");
            }

            if (error)
            {
                // if we have errors send details to OTRS
                string errsubject = "Errors during upload";
                string errmessage = JArray.FromObject(exceptions).ToString();

                CreateNewTicket otrs = new CreateNewTicket();

                Console.WriteLine("Upload had errors - please see ticketing");

                try
                {
                    await otrs.CreateTicketAsync(errsubject, errmessage, Customer: otrssettings.customer, Queue: otrssettings.errqueue);
                }
                catch (TaskCanceledException)
                {
                    //unable to create ticket so print to screen
                    Console.WriteLine("Unable to create ticket");
                    Console.WriteLine("Subject: " + errsubject);
                    Console.WriteLine("Message:");
                    Console.Write(errmessage);
                    Console.WriteLine("");
                }
            }
            else 
            {
                Console.WriteLine("Upload successful");
                Console.WriteLine("Completed at " + DateTime.Now.ToString());
            }

            return true;
        }
    }

    public class OCWebRequest {
        public HttpClient moodle { get; set; }
        public HttpResponseMessage httpresponse { get; set; }
        public string resource { get; set; }
        private UriBuilder resourceuri;
        private NameValueCollection querystring;
        public string response { get; set; }

        // Define private variables for username and password, and set defaults.
        private string _token;

        // Create write-only properties to allow access to set token.
        public string token
        {
            set
            {
                _token = value;
            }
        }

        public OCWebRequest()
        {
            moodle = new HttpClient();
            httpresponse = new HttpResponseMessage();

            // Get default base address and token from saved properties
            moodle.BaseAddress = new Uri(Properties.moodlerest.Default.MoodleBase);
            if (!string.IsNullOrWhiteSpace(Properties.moodlerest.Default.MoodleToken))
            {
                byte[] enc = System.Convert.FromBase64String(Properties.moodlerest.Default.MoodleToken);
                byte[] entropy = System.Convert.FromBase64String(Properties.moodlerest.Default.MEntropy);
                byte[] unenc = ProtectedData.Unprotect(enc, entropy, DataProtectionScope.CurrentUser);
                _token = Encoding.Unicode.GetString(unenc);
            }
        }

        // Request() method runs the REST request returning the HttpStatusCode 
        public async Task<HttpStatusCode> Send<T>(T request)
        {
            JToken responsejson;
            // Build uri from base request and resource and add username and password to query string.
            resourceuri = new UriBuilder(moodle.BaseAddress);
            querystring = HttpUtility.ParseQueryString(resourceuri.Query);

            querystring["wstoken"] = _token;

            resourceuri.Query = querystring.ToString();

            short retries = 0;

            while (true)
            {
                try
                {
                    // Send REST request to OTRS.
                    httpresponse = await moodle.PostAsJsonAsync(resourceuri.Uri.ToString(), request);
                    httpresponse.EnsureSuccessStatusCode();

                    // Process response.
                    response = await httpresponse.Content.ReadAsStringAsync();

                    responsejson = null;
                    try
                    {
                        responsejson = JToken.Parse(response);

                        if (responsejson.SelectToken("exception") != null && !String.IsNullOrWhiteSpace(responsejson.SelectToken("exception").ToString()))
                        {
                            // Error, so check if we should retry
                            retries++;
                            Console.WriteLine("Connection error");
                            if (retries > 2) break;
                            Console.WriteLine("Retry " + retries);
                            continue;
                        }
                    }
                    catch(JsonReaderException e)
                    {
                        // Log HTTP Request errors to OTRS and then re-throw for calling function to handle fallbacks.

                        CreateNewTicket otrs = new CreateNewTicket();
                        string subject = "Moodle REST Library:  Error Communicating with Moodle";
                        string message = "Error communicating with Moodle (Invalid JSON Response)" + System.Environment.NewLine + System.Environment.NewLine;

                        message += "Exception: " + e.Message + System.Environment.NewLine + System.Environment.NewLine;

                        message += "HTTP Response Status: " + httpresponse.StatusCode.ToString() + " " + httpresponse.ReasonPhrase + System.Environment.NewLine + System.Environment.NewLine;

                        message += "Request: " + System.Environment.NewLine + JToken.FromObject(request).ToString() + System.Environment.NewLine + System.Environment.NewLine;

                        message += "Response Headers: " + System.Environment.NewLine + string.Join(System.Environment.NewLine, httpresponse.Headers.Select(x => "\t" + x.Key + "=" + String.Join(Environment.NewLine,x.Value))) + System.Environment.NewLine + System.Environment.NewLine;

                        try
                        {
                            otrs.CreateTicket(subject, message, Customer: otrssettings.customer, Queue: otrssettings.errqueue);
                        }
                        catch (TaskCanceledException)
                        {
                            //unable to create ticket so print to screen
                            Console.WriteLine("Unable to create ticket");
                            Console.WriteLine("Subject: " + subject);
                            Console.WriteLine("Message:");
                            Console.Write(message);
                            Console.WriteLine("");
                        }


                        System.Diagnostics.Debug.WriteLine(message);
                    }

                    break;
                }
                catch (HttpRequestException e)
                {
                    // Log HTTP Request errors to OTRS and then re-throw for calling function to handle fallbacks.

                    CreateNewTicket otrs = new CreateNewTicket();
                    string subject = "Moodle REST Library:  Error Communicating with Moodle";
                    string message = "Error communicating with Moodle" + System.Environment.NewLine + System.Environment.NewLine;

                    message += "Exception: " + e.Message + System.Environment.NewLine + System.Environment.NewLine;

                    message += "HTTP Response Status: " + httpresponse.StatusCode.ToString() + " " + httpresponse.ReasonPhrase + System.Environment.NewLine + System.Environment.NewLine;

                    message += "Request: " + System.Environment.NewLine + JToken.FromObject(request).ToString() + System.Environment.NewLine + System.Environment.NewLine;

                    message += "Response Headers: " + System.Environment.NewLine + string.Join(System.Environment.NewLine, httpresponse.Headers.Select(x => "\t" + x.Key + "=" + x.Value)) + System.Environment.NewLine + System.Environment.NewLine;

                    try
                    {
                        otrs.CreateTicket(subject, message, Customer: otrssettings.customer, Queue: otrssettings.errqueue);
                    }
                    catch (TaskCanceledException)
                    {
                        //unable to create ticket so print to screen
                        Console.WriteLine("Unable to create ticket");
                        Console.WriteLine("Subject: " + subject);
                        Console.WriteLine("Message:");
                        Console.Write(message);
                        Console.WriteLine("");
                    }
                    

                    System.Diagnostics.Debug.WriteLine(message);

                    throw e;
                }
                catch(TaskCanceledException)
                {
                    // Connection timed out, so check if we should retry
                    retries++;
                    Console.WriteLine("Connection timeout");
                    if (retries > 2) throw;
                    Console.WriteLine("Retry " + retries);
                }
            }

            if(!String.IsNullOrWhiteSpace(response) && response != "null")
            {
                // Check response for errors and warnings - setup a ticket in case we need one.


                CreateNewTicket otrs = new CreateNewTicket();
                bool responseticket = false;
                string subject = "Moodle REST Library: REST Webservice Error";
                string message = "";
                if (responsejson.SelectToken("exception") != null && !String.IsNullOrWhiteSpace(responsejson.SelectToken("exception").ToString()))
                {
                    // There is an error, so setup a ticket for it
                    responseticket = true;
                    subject = "Moodle REST Library:  REST Webservice Error";
                    message = "REST Errors Detected" + System.Environment.NewLine + System.Environment.NewLine;
                    message += "Payload:" + System.Environment.NewLine + JToken.FromObject(request).ToString() + System.Environment.NewLine + System.Environment.NewLine;
                    message += "Response:" + System.Environment.NewLine + responsejson.ToString();
                }
                else if(responsejson.SelectToken("warnings") != null && responsejson.SelectToken("warnings").AsEnumerable().Count() > 0)
                {
                    // There is a warning, so setup a ticket for it
                    responseticket = true;
                    subject = "Moodle REST Library:  REST Webservice Warning";
                    message = "REST Warnings Detected" + System.Environment.NewLine + System.Environment.NewLine;
                    message += "Payload:" + System.Environment.NewLine + JToken.FromObject(request).ToString() + System.Environment.NewLine + System.Environment.NewLine;
                    message += "Response:" + System.Environment.NewLine + responsejson.ToString();
                }


                if(responseticket) 
                {
                    // If we had an error or warning and setup a ticket, then send it to OTRS.
                    try
                    {
                        otrs.CreateTicket(subject, message, Customer: otrssettings.customer, Queue: otrssettings.errqueue);
                    }
                    catch (TaskCanceledException)
                    {
                        //unable to create ticket so print to screen
                        Console.WriteLine("Unable to create ticket");
                        Console.WriteLine("Subject: " + subject);
                        Console.WriteLine("Message:");
                        Console.Write(message);
                        Console.WriteLine("");
                    }
                }
            }

            // Return the HTTP Status Code.
            return httpresponse.StatusCode;
        }
    }

    public class Username
    {
        private Dictionary<string, int> genusernames;

        public Username() 
        {
            genusernames = new Dictionary<string, int>();
        }

        // Static class for generating usernames.
        public async Task<String> Generate ( string usernameprefix)
        {
            // generate username.
            OCWebRequest moodle = new OCWebRequest();
            lkupidnumber userlookup;
            Regex usernamergx;
            JToken jsonresponse;
            List<int> suffixes;
            int suffix;
            string newsuffix;
            string returnval;
            
 
            // Remove whitespace and convert usernameprefix to lowercase
            
            usernameprefix = usernameprefix.Trim().ToLower().Replace(" ", String.Empty);

            // Regular expression, eliminate all chars EXCEPT:
            // alphanum, dash (-), underscore (_) and period (.) characters.

            usernamergx = new Regex("[^-\\._a-z0-9]");
            usernameprefix = usernamergx.Replace(usernameprefix, String.Empty);

            // Transfer suffixes from ends of usernames to new List.
            suffixes = new List<int>();

            if (genusernames.ContainsKey(usernameprefix))
            {
                suffixes.Add(genusernames[usernameprefix]);
            }
            else
            {
                // Lookup any users currently with that prefix.
                userlookup = new lkupidnumber();
                userlookup.criteria.Add(new lkcriteria() { key = "username", value = usernameprefix + "%" });

                await moodle.Send(userlookup);


                // Process response into JToken object.
                jsonresponse = JToken.Parse(moodle.response);
                if (jsonresponse["users"] != null)
                {
                    jsonresponse = jsonresponse["users"];


                    if (jsonresponse.HasValues)
                    {
                        foreach (JToken user in jsonresponse.Children())
                        {
                            if (int.TryParse(user["username"].ToString().Substring(usernameprefix.Length), out suffix))
                            {
                                suffixes.Add(suffix);
                            }
                        }
                    }
                }
            }

            // If there are suffixes, then add one to the largest.
            if (suffixes.Count() > 0)
            {
                // find next number
                newsuffix = (suffixes.Max() + 1).ToString("D2"); // new suffix string with required leading zeros as necessary.
                returnval = usernameprefix + newsuffix;
                genusernames[usernameprefix] = suffixes.Max() + 1;
            }
            else
            {
                // else start at 01.
                returnval = usernameprefix + "01";
                genusernames[usernameprefix] = 1;
            }

            return returnval;
        }
    }

    public class geonames
    {
        // Class for looking up geonames data.
        public HttpClient geonamehttp { get; set; }
        public HttpResponseMessage httpresponse { get; set; }
        public string resource { get; set; }
        private UriBuilder resourceuri;
        private NameValueCollection querystring;
        public string response { get; set; }

        // Define private variables for username and password, and set defaults.
        public string username { get; set; }
        private string _password;

        // Create write-only properties to allow access to set token.
        public string password
        {
            set
            {
                _password = value;
            }
        }

        public geonames()
        {
            geonamehttp = new HttpClient();
            geonamehttp.BaseAddress = new Uri("http://api.geonames.org/");
            httpresponse = new HttpResponseMessage();

            username = Properties.moodlerest.Default.GeoUser;
            if (!string.IsNullOrWhiteSpace(Properties.moodlerest.Default.GeoPass))
            {
                byte[] enc = System.Convert.FromBase64String(Properties.moodlerest.Default.GeoPass);
                byte[] entropy = System.Convert.FromBase64String(Properties.moodlerest.Default.GEntropy);
                byte[] unenc = ProtectedData.Unprotect(enc, entropy, DataProtectionScope.CurrentUser);
                _password = Encoding.Unicode.GetString(unenc);
            }
        }

        // Request() method runs the REST request returning the HttpStatusCode 
        public async Task<HttpStatusCode> Send(NameValueCollection request)
        {
            // Build uri from base request and resource and add username and password to query string.
            resourceuri = new UriBuilder(geonamehttp.BaseAddress+resource);
            querystring = HttpUtility.ParseQueryString(resourceuri.Query);

            querystring.Add(request);

            querystring["username"] = username;
            querystring["password"] = _password;

            resourceuri.Query = querystring.ToString();

            short retries = 0;

            // Send REST request to OTRS.
            while (true)
            {
                try
                {
                    httpresponse = await geonamehttp.GetAsync(resourceuri.Uri);
                    httpresponse.EnsureSuccessStatusCode();  // Throw exception if not a success code.

                    // Process response.
                    response = await httpresponse.Content.ReadAsStringAsync();
                    break;
                }
                catch (HttpRequestException e)
                {
                    // Log HTTP Request errors to OTRS and then re-throw for calling function to handle fallbacks.

                    CreateNewTicket otrs = new CreateNewTicket();
                    string subject = "Moodle REST Library:  Error Communicating with Geonames";
                    string message = "Error communicating with Geonames" + System.Environment.NewLine + System.Environment.NewLine;

                    message += "Exception: " + e.Message + System.Environment.NewLine + System.Environment.NewLine;

                    message += "HTTP Response Status: " + httpresponse.StatusCode.ToString() + " " + httpresponse.ReasonPhrase + System.Environment.NewLine + System.Environment.NewLine;

                    message += "Request: " + System.Environment.NewLine + string.Join(System.Environment.NewLine, request.AllKeys.Select(x => "\t" + x + "=" + request[x])) + System.Environment.NewLine + System.Environment.NewLine;

                    message += "Response Headers: " + System.Environment.NewLine + string.Join(System.Environment.NewLine, httpresponse.Headers.Select(x => "\t" + x.Key + "=" + x.Value)) + System.Environment.NewLine + System.Environment.NewLine;

                    try
                    {
                        otrs.CreateTicket(subject, message, Customer: otrssettings.customer, Queue: otrssettings.errqueue);
                    }
                    catch (TaskCanceledException)
                    {
                        //unable to create ticket so print to screen
                        Console.WriteLine("Unable to create ticket");
                        Console.WriteLine("Subject: " + subject);
                        Console.WriteLine("Message:");
                        Console.Write(message);
                        Console.WriteLine("");
                    }

                    System.Diagnostics.Debug.WriteLine(message);

                    throw e;
                }
                catch (TaskCanceledException)
                {
                    // Connection timed out, so check if we should retry
                    retries++;
                    Console.WriteLine("Connection timeout");
                    if (retries > 2) throw;
                    Console.WriteLine("Retry " + retries);
                }
            }

            // Return the HTTP Status Code.
            return httpresponse.StatusCode;
        }
    }

    public static class timezone
    {
        // static class for looking up timezones.
        public static async Task<string> Lookup (string city, string country, double fuzzy = 0.8)
        {
            geonames gnsearch = new geonames();
            geonames tzlookup = new geonames();
            string returntimezone = "";

            // Setup geonames data.
            gnsearch.resource = "searchJSON";

            tzlookup.resource = "timezoneJSON";

            if (!String.IsNullOrWhiteSpace(city))
            {
                // If we have a city, try looking that up first.

                NameValueCollection search = new NameValueCollection();

                search["q"] = city;
                search["country"] = country;
                search["fuzzy"] = fuzzy.ToString();
                search["orderby"] = "relevance";

                try { 
                    await gnsearch.Send(search);

                    JObject searchresults = JObject.Parse(gnsearch.response);

                    int noresults;

                    // Process results.
                    if (searchresults["totalResultsCount"] != null && Int32.TryParse(searchresults["totalResultsCount"].ToString(), out noresults))
                    {
                        if (noresults > 0 && searchresults["geonames"] != null)
                        {
                            // Check to see if there are results.

                            foreach (JToken place in searchresults["geonames"].Children())
                            {
                                if (place["lat"] != null && place["lng"] != null)
                                {
                                    NameValueCollection timezoneloc = new NameValueCollection();

                                    // Lookup timezone from lat/lng figures for current result from geonames search.

                                    timezoneloc["lat"] = place["lat"].ToString();
                                    timezoneloc["lng"] = place["lng"].ToString();

                                    await tzlookup.Send(timezoneloc);

                                    JObject timezonedata = JObject.Parse(tzlookup.response);

                                    if (timezonedata["timezoneId"] != null)
                                    {
                                        returntimezone = timezonedata["timezoneId"].ToString();
                                        break;  // If we've found a timezone stop.  Otherwise continue loop with next result.
                                    }
                                }
                            }
                        }
                    }
                }
                catch (HttpRequestException)
                {
                    // Ignore HTTP Request exceptions as we'll be trying another HTTP Request below and will have already logged these in the geonames.Send function.
                }
                catch (TaskCanceledException)
                {
                    // Ignore timeouts
                }
            }

            if(String.IsNullOrEmpty(returntimezone) && ! String.IsNullOrWhiteSpace(country))
            {
                // Couldn't find timezone for city, so try for country's capital instead.
                geonames capitallookup = new geonames();

                capitallookup.resource = "countryInfoJSON";

                NameValueCollection countryinfo = new NameValueCollection();

                countryinfo["country"] = country;

                try
                {
                    await capitallookup.Send(countryinfo);

                    JObject capitalresults = JObject.Parse(capitallookup.response);

                    if (capitalresults["geonames"] != null && capitalresults["geonames"].HasValues && capitalresults["geonames"].First()["capital"] != null && capitalresults["geonames"].First()["capital"].ToString() != city)
                    {
                        // Try again with countries capital if it's different to the location just searched.
                        returntimezone = await timezone.Lookup(capitalresults["geonames"].First()["capital"].ToString(), country);
                    }
                }
                catch (HttpRequestException)
                {
                    // Ignore HTTP Request exceptions as we will have already logged these in the geonames.Send function.
                    // Default value for timezone will be set below.
                }
                catch (TaskCanceledException)
                {
                    // Ignore timeouts and use default value as set below.
                }
            }

            if(String.IsNullOrWhiteSpace(returntimezone))
            {
                // Still couldn't find a timezone, so default to London.
                returntimezone = "Europe/London";
            }

            return returntimezone;
        }
    }
}
