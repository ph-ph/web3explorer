import React from 'react';
import { useParams } from "react-router-dom";
import './App.css';

import 'bootstrap/dist/css/bootstrap.min.css';

import BTable from 'react-bootstrap/Table';
import { Link } from "react-router-dom";

import { initializeApp } from "firebase/app";
import {
  getFirestore,
  getDoc,
  doc
} from 'firebase/firestore/lite';

// Let's use anonymous authentication to prevent scripted access to our Firestore data
import { getAuth, signInAnonymously } from "firebase/auth";
import { useSortBy, useTable } from 'react-table'

// Your web app's Firebase configuration
const firebaseConfig = {
  apiKey: "AIzaSyCrp3c1DBj63oSJc1tos_pSkCCWCvpylLs",
  authDomain: "web3twitterdata.firebaseapp.com",
  projectId: "web3twitterdata",
  storageBucket: "web3twitterdata.appspot.com",
  messagingSenderId: "794734525030",
  appId: "1:794734525030:web:0364e799559704951acba7"
};

const RANGE_OPTIONS = [
  {
    title: "Last Month",
    value: "lastMonth",
  },
  {
    title: "Last Week",
    value: "lastWeek",
  },
  {
    title: "Last 2 Days",
    value: "last2days",
  }
];
const ACCEPTABLE_RANGE_VALUES = RANGE_OPTIONS.map(option => option.value);

// This is super UGLY, but I need to share the state between App and TweetsView, and that's the easiest way that I know
var URLS_GLOBAL = [];

// Initialize Firebase
initializeApp(firebaseConfig);
const db = getFirestore();
const auth = getAuth();

/**
 *
 * @param {string} range - document key. See ACCEPTABLE_RANGE_VALUES for options.
 * @returns array of structures describing popular urls and their stats
 */
async function getPopularUrls(range) {
  if (!ACCEPTABLE_RANGE_VALUES.includes(range)) {
    console.error("Invalid range value", range);
    return [];
  }
  const docSnap = await getDoc(doc(db, "urlsData", range));

  if (docSnap.exists()) {
    const urls = docSnap.data().urls;
    // let's add index to each url so that we can link to "view tweets" page
    urls.forEach((url, i) => { url.index = i; });
    return urls;
  } else {
    console.error("The document doesn't exist", range);
    return [];
  }
}

/**
 * Get the current value for date range from url.
 * @returns one of the values from ACCEPTABLE_RANGE_VALUES
 */
function getRangeParameter() {
  const windowUrl = window.location.search;
  const params = new URLSearchParams(windowUrl);
  const range = params.get("range");
  if (!range || !ACCEPTABLE_RANGE_VALUES.includes(range)) {
    return "lastMonth";
  }
  return range;
}

/**
 * Simple React component that wraps children in <b></b> if condition is true.
 */
function ConditionalBold({condition, children}) {
  if (condition) {
    return (<b>{children}</b>);
  } else {
    return children;
  }
}

/**
 * React component to render date range selector.
 * @param {string} selectedRange - currently selected date range, a value from ACCEPTABLE_RANGE_VALUES
 */
function RangeSelector({selectedRange}) {
  const rangeLinks = RANGE_OPTIONS.map(option => {
    return (
      <span key={option.value}>
        <a href={"?range=" + option.value}>
          <ConditionalBold condition={option.value === selectedRange}>
            {option.title}
          </ConditionalBold>
        </a>
        &nbsp;
      </span>
    );
});
  return (
    <div>
      Date range: {rangeLinks}
    </div>
  );
}

/**
 * React component that displays a table with popular urls and their stats.
 *
 * @param {struct[]} urls - array of structures describing popular urls and their statistics
 * @returns
 */
function UrlsTable({urls}) {
  // table column definitions
  const columns = React.useMemo(() => [
    {
      Header: 'URL',
      accessor: (originalRow, rowIndex) => ({
        url: originalRow['mentioned_url'],
        title: originalRow['title'],
      }),
      Cell: ({value}) => {
        const {url, title} = value;
        return (<a href={url}>{title}</a>);
      },
    },
    {
      Header: 'Mentions',
      accessor: 'mentions_count',
    },
    {
      Header: 'Mentions by influencers',
      accessor: 'influencer_mentions_count',
    },
    {
      Header: 'Quotes',
      accessor: 'quote_count',
    },
    {
      Header: 'Retweets',
      accessor: 'retweet_count',
    },
    {
      Header: 'Mentioned by',
      accessor: 'mentioned_by_influencers',
      Cell: ({value}) => {
        const influencerProfiles = value.map(username => (<span key={username}><a href={"https://twitter.com/" + username}>@{username}</a>&nbsp;</span>));
        return (<span>{influencerProfiles}</span>)
      },
    },
    {
      Header: 'Tweets',
      accessor: 'index',
      Cell: ({value}) => {
        return (<Link to={"/tweets/" + value}>View</Link>);
      }
    }
  ], []);

  const {
    getTableProps,
    getTableBodyProps,
    headerGroups,
    rows,
    prepareRow,
  } = useTable({
    columns,
    data: urls,
  }, useSortBy);

  return (
    <BTable striped bordered hover size="sm" {...getTableProps()}>
      <thead>
        {headerGroups.map(headerGroup => (
          <tr {...headerGroup.getHeaderGroupProps()}>
            {headerGroup.headers.map(column => (
              // Add the sorting props to control sorting. For this example
                // we can add them into the header props
                <th {...column.getHeaderProps(column.getSortByToggleProps())}>
                  {column.render('Header')}
                  {/* Add a sort direction indicator */}
                  <span>
                    {column.isSorted
                      ? column.isSortedDesc
                        ? ' 🔽'
                        : ' 🔼'
                      : ''}
                  </span>
                </th>
            ))}
          </tr>
        ))}
      </thead>
      <tbody {...getTableBodyProps()}>
        {rows.map((row, i) => {
          prepareRow(row)
          return (
            <tr {...row.getRowProps()}>
              {row.cells.map(cell => {
                return <td {...cell.getCellProps()}>{cell.render('Cell')}</td>
              })}
            </tr>
          )
        })}
      </tbody>
    </BTable>
  );
}

export class App extends React.Component {
  constructor(props) {
    super(props);
    this.state = {
      urls: [],
      range: getRangeParameter(),
    }
  }

  componentDidMount() {
    // Sign in the user into Firestore/Firebase
    signInAnonymously(auth)
    .then(() => {
      // Signed in..
      console.log("user signed in");
      // Load popular urls
      getPopularUrls(this.state.range).then((urls) => {
        URLS_GLOBAL = urls;
        this.setState((state, props) => ({ urls }));
      });
    })
    .catch((error) => {
      const errorCode = error.code;
      const errorMessage = error.message;
      console.error("failed to anonymously sign in the user", errorCode, errorMessage);
    });
  }

  render() {
    return (
      <div className="App">
        <br/>
        <RangeSelector selectedRange={this.state.range} />
        <br/>
        <UrlsTable urls={this.state.urls} />
      </div>
    );
  }
}

export function TweetsView() {
  const params = useParams();
  const urlData = URLS_GLOBAL.find(url => (url.index === parseInt(params.urlId, 10)));
  if (!urlData) {
    return (<div>URL not found! {params.urlId}</div>);
  }
  const tweets = urlData.tweet_urls.map(url => (<div key={url}><a href={url}>{url}</a></div>));
  return (
    <div>{tweets}</div>
  );
}
