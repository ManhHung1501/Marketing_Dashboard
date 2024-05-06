$(document).ready(function () {
    let myChart;
    let table;
    let chartTabActive = '';

    const swiper = new Swiper('.swiper', {
        slidesPerView: 5,
        spaceBetween: 1,
        navigation: {
            nextEl: '.stats-chart-next',
            prevEl: '.stats-chart-prev',
        },
        allowTouchMove: false,
    });

    table = $('#example').DataTable({
        fixedHeader: true,
        scrollX: true,
        buttons: [
            {
                extend: 'csv',
                text: 'CSV',
                exportOptions: {
                    columns: ':visible'
                },
            },
            {
                extend: 'excel',
                text: 'Excel',
                exportOptions: {
                    columns: ':visible'
                },
            },
        ],
        fixedColumns: {
            left: 1,
        },
        dom: '<"top-table"fB>rt<"bottom"<"wrap-bottom"<"wrap-bottom-left"i><"wrap-bottom-right"<"me-2"l>pu>>><"clear">',
        lengthMenu: [
            [10, 25, 50, 100, -1],
            [10, 25, 50, 100, 'All'],
        ],
        initComplete: function () {

        },
        drawCallback: function () {
            sumTotal(this);

            $('.table-wrap table tbody tr td').each((index, item) => {
                let gameName = $(item).parent().find('td:nth-child(2) .game-meta').text();
                $(item).attr('data-bs-toggle', 'tooltip');
                $(item).attr('data-bs-title', gameName || 'Loading');
            })

            const tooltipTriggerList = document.querySelectorAll('[data-bs-toggle="tooltip"]')
            const tooltipList = [...tooltipTriggerList].map(tooltipTriggerEl => new bootstrap.Tooltip(tooltipTriggerEl))
        },
        columnDefs: [
            {
                targets: 0,
                "render": function (data, type, row, meta) {
                    return `<span class="d-none sub-data-row"></span>`
                },
                orderable: false,
            },
            {
                targets: 1,
                "render": function (data, type, row, meta) {
                    return `<span class="game-meta d-flex justify-content-between">
                                <span>
                                    ${data} (${row.platform})
                                    <a href="${row.link}" target="_blank">
                                        <i class="bi bi-link-45deg"></i>
                                    </a>
                                </span>
                                ${row.new ? '<span class="badge text-success fw-bold d-flex align-items-center">New</span>' : ''}
                            </span>`
                },
            },
            {
                targets: 7,
                "render": function (data, type, row, meta) {
                    return `<span class="text-end d-block">${data}</span>`
                },
            },
            {
                targets: 8,
                "render": function (data, type, row, meta) {
                    return `<span class="text-end d-block">${data}</span>`
                },
            },
            {
                targets: 9,
                "render": function (data, type, row, meta) {
                    return `<span class="text-end d-block">${data}</span>`
                },
            },
            {
                targets: 10,
                "render": function (data, type, row, meta) {
                    return `<span class="text-end d-block">${data}</span>`
                },
            },
            {
                targets: 11,
                "render": function (data, type, row, meta) {
                    return `<span class="text-end d-block">${data}</span>`
                },
            },
            {
                targets: 12,
                "render": function (data, type, row, meta) {
                    return `<span class="text-end d-block">${data}</span>`
                },
            },
            {
                targets: 13,
                "render": function (data, type, row, meta) {
                    return `<span class="text-end d-block">${data}</span>`
                },
            },
            {
                targets: 14,
                "render": function (data, type, row, meta) {
                    return `<span class="text-end d-block">${data}</span>`
                },
            },
        ],
        ajax: {
            url: '/loaddata/api/',
            dataSrc: function (json) {
                switch ($('#p-breakby').val()) {
                    case 'game_name':
                        return json.data.filter((item, index) => item.game !== 'sum');
                        break;
                    case 'date':
                        return json.data.filter((item, index) => item.date !== 'sum');
                        break;
                    case 'country_name':
                        return json.data.filter((item, index) => item.country !== 'sum');
                        break;
                    case 'platform':
                        return json.data.filter((item, index) => item.platform !== 'sum');
                        break;
                    case 'network':
                        return json.data.filter((item, index) => item.network !== 'sum');
                        break;
                    case 'company':
                        return json.data.filter((item, index) => item.company !== 'sum');
                        break;
                }
            },
            data: function (d) {
                d[$('#p-game-name').attr("name")] = $('#p-game-name').val();
                d[$('#p-platform').attr("name")] = $('#p-platform').val();
                d[$('#p-country').attr("name")] = $('#p-country').val();
                d[$('#p-adsource').attr("name")] = $('#p-adsource').val();
                d[$('#p-company').attr("name")] = $('#p-company').val();
                d[$('#Sdate').attr("name")] = $('#Sdate').val();
                d[$('#Edate').attr("name")] = $('#Edate').val();
                d[$('#p-breakby').attr("name")] = $('#p-breakby').val();
            },
            error: async function (jqXHR, ajaxOptions, thrownError) {
                console.log(thrownError);
                let result = await Swal.fire({
                    title: 'Error!',
                    text: 'An error occurred, please press F5 to reload the page!',
                    icon: 'error',
                    confirmButtonText: 'Close'
                })

                if (result.isConfirmed) {
                    $('.loading-panel').addClass('inactive')
                    $('.loading-panel').removeClass('active')
                }
            }
        },
        columns: [
            {
                className: 'dt-control',
                orderable: false,
                searchable: false,
                data: null,
                defaultContent: '',
            },
            {
                data: function (row, type, set) {
                    return row.game ?? '';
                }
            },
            {
                data: function (row, type, set) {
                    return row.date ?? '';
                }
            },
            {
                data: function (row, type, set) {
                    return row.country ?? '';
                }
            },
            {
                data: function (row, type, set) {
                    return row.platform ?? '';
                }
            },
            {
                data: function (row, type, set) {
                    return row.company ?? '';
                }
            },
            {
                data: function (row, type, set) {
                    return row.network ?? '';
                }
            },
            {
                data: function (row, type, set) {
                    return row.revenue_in_app ? numeral(row.revenue_in_app).format('0,0.00 $') : '0.00 $';
                }
            },
            {
                data: function (row, type, set) {
                    return row.revenue_ads ? numeral(row.revenue_ads).format('0,0.00 $') : '0.00 $';
                }
            },
            {
                data: function (row, type, set) {
                    return row.revenue_sum ? numeral(row.revenue_sum).format('0,0.00 $') : '0.00 $';
                }
            },
            {
                data: function (row, type, set) {
                    return row.cost ? numeral(row.cost).format('0,0.00 $') : '0.00 $';
                }
            },
            {
                data: function (row, type, set) {
                    return row.profit ? numeral(row.profit).format('0,0.00 $') : '0.00 $';
                }
            },
            {
                data: function (row, type, set) {
                    return row.impression ?? 0;
                }
            },
            {
                data: function (row, type, set) {
                    return row.ecpm ?? 0;
                }
            },
            {
                data: function (row, type, set) {
                    return row.roas ?? 0;
                }
            },
        ],
    });

    function resetSlideByBreak(slideRemove) {
        swiper.removeAllSlides();
        swiper.appendSlide([
            '<div class="swiper-slide"><div class="stats-chart-item" data-tab-value="revenue_in_app"><span class="tab-title">Revenue In App (Sum)</span><span class="tab-sum-value">$----</span></div></div>',
            '<div class="swiper-slide"><div class="stats-chart-item" data-tab-value="revenue_ads"><span class="tab-title">Revenue Ads (Sum)</span><span class="tab-sum-value">$----</span></div></div>',
            '<div class="swiper-slide"><div class="stats-chart-item" data-tab-value="revenue_sum"><span class="tab-title">Revenue All (Sum)</span><span class="tab-sum-value">$----</span></div></div>',
            '<div class="swiper-slide"><div class="stats-chart-item" data-tab-value="cost"><span class="tab-title">Cost (Sum)</span><span class="tab-sum-value">$----</span></div></div>',
            '<div class="swiper-slide"><div class="stats-chart-item" data-tab-value="profit"><span class="tab-title">Profit (Sum)</span><span class="tab-sum-value">$----</span></div></div>',
            '<div class="swiper-slide"><div class="stats-chart-item" data-tab-value="revenue"><span class="tab-title">Revenue (Sum)</span><span class="tab-sum-value">$----</span></div></div>',
            '<div class="swiper-slide"><div class="stats-chart-item" data-tab-value="impression"><span class="tab-title">Impressions (Sum)</span><span class="tab-sum-value">$----</span></div></div>',
            '<div class="swiper-slide"><div class="stats-chart-item" data-tab-value="ecpm"><span class="tab-title">ECPM (Mean)</span><span class="tab-sum-value">$----</span></div></div>',
            '<div class="swiper-slide"><div class="stats-chart-item" data-tab-value="roas"><span class="tab-title">ROAS (Mean)</span><span class="tab-sum-value">$----</span></div></div>'
        ]);
        swiper.removeSlide(slideRemove);
    }

    function showChartLabelList(myChart) {
        let items = myChart.legend.legendItems;

        $('.chart-sidebar-list').empty();

        if ($('#p-breakby').val() === 'date') return;


        items.forEach((item, index) => {
            $('.chart-sidebar-list').append(`
            <div class="form-check" id="chart_label_${index}" data-revenue=${myChart.data.datasets[index].revenue}  data-label="${item.text}" :class='$el.dataset.label.toLowerCase().indexOf(searchKey.toLowerCase()) > - 1 ? "" : "hide"'>
                <input class="form-check-input chart-list-item label-chart-${index}" type="checkbox" value="${index}"
                    id="chart_label_${index}">
                <label class="form-check-label form-check-label-chart" for="chart_label_${index}">
                    ${item.text}
                </label>
            </div>`);
        })

        function changeDataset(item) {
            if (!item.is(":checked")) {
                myChart.hide(Number(item.val()))
            } else {
                myChart.show(Number(item.val()))
            }
        }

        $('.chart-list-item').each(function (index, item) {
            $(item).change(function () {
                changeDataset($(this));
                setTimeout(() => {
                    myChart.options.scales.y.ticks.stepSize = Math.floor(myChart.scales.y.max / 4);
                    myChart.update();
                }, 0);
            })
        });
    }

    function format(d) {
        let html = '';
        let subTableHead =
            '<thead>' +
            '<tr>' +
            '<th>ECPM</th>' +
            '<th>Impression</th>' +
            '<th>Installs</th>' +
            '<th>ActivityRevenue</th>' +
            '<th>ROI</th>' +
            '<th>AverageECPI</th>' +
            '<th>UninstallRate</th>' +
            '</tr>' +
            '</thead>';

        let subTableRow =
            '<tr>' +
            '<td>' +
            d.subdata_ecpm +
            '</td>' +
            '<td>' +
            d.subdata_impression +
            '</td>' +
            '<td>' +
            d.subdata_installs +
            '</td>' +
            '<td>' +
            d.subdata_activity_revenue +
            '</td>' +
            '<td>' +
            d.subdata_roi +
            '</td>' +
            '<td>' +
            d.subdata_average_ecpi +
            '</td>' +
            '<td>' +
            d.subdata_uninstall_rate +
            '</td>' +
            '</tr>';

        html += '<table class="table mb-0 border table-dark table-subrow">';
        html += '<tbody>';
        html += subTableHead;
        html += subTableRow;
        html += '</tbody>';
        html += '</table>';

        return html;
    }

    function sumTotal(context) {
        let api = context.api();

        let columnVisible = api.columns(':visible')[0];
        for (let col_index = 0; col_index < columnVisible.length; col_index++) {
            if (![0, 1, 2, 3, 4, 5, 6, 7].includes(columnVisible[col_index])) {
                let pageTotal = api.column(columnVisible[col_index], { page: 'all' }).data().sum();

                let columnFooter = $('.dataTables_scrollFoot tfoot tr:eq(0) th:eq(' + col_index + ')');

                // console.log('footer pageTotal', pageTotal);

                if (columnFooter.hasClass('normal-number')) {
                    columnFooter.html(Math.round(pageTotal * 100) / 100);
                } else {
                    columnFooter.html(
                        `${numeral(Math.round(pageTotal * 100) / 100).format(
                            '0,0.00 $'
                        )}`
                    )
                }
            }
        }
    }

    function hideColumn(columnArr) {
        table.columns(columnArr).visible(false);
    }

    function showColumn(columnArr) {
        table.columns(columnArr).visible(true);
    }

    function showColumnByBreak() {
        switch ($('#p-breakby').val()) {
            case 'game_name':
                showColumn([1, 7, 8, 9, 10, 11]);
                hideColumn([2, 3, 4, 5, 6, 12, 13]);
                break;
            case 'date':
                hideColumn([1, 3, 4, 5, 6, 12, 13]);
                showColumn([2, 7, 8, 9, 10, 11]);
                break;
            case 'country_name':
                hideColumn([1, 2, 4, 5, 6, 12, 13]);
                showColumn([3, 7, 8, 9, 10, 11]);
                break;
            case 'platform':
                hideColumn([1, 2, 3, 5, 6, 12, 13]);
                showColumn([4, 7, 8, 9, 10, 11]);
                break;
            case 'network':
                hideColumn([1, 2, 3, 4, 5, 7, 9, 10, 11]);
                showColumn([6, 8, 12, 13]);
                break;
            case 'company':
                hideColumn([1, 2, 3, 4, 6, 12, 13]);
                showColumn([5, 7, 8, 9, 10, 11]);
                break;
            default:
                hideColumn([2, 3, 4, 5, 6, 12, 13]);
                showColumn([1, 7, 8, 9, 10, 11]);
        }

        table.columns.adjust().draw();
        // $('.th-total').attr('colspan', 2);
    }

    function filterDataBreakGame(json) {
        return json.data.map((item, index) => ({
            game_id: item.game_id,
            game_name: item.game,
            platform: item.platform,
            data_by_date: item.data_by_date.map((item, index) => ({
                ...item,
                date: moment(item.date).format("MMM DD, YYYY"),
            })),
            revenue: item.revenue_ads
        }))
    }

    function filterDataBreakDate(json) {
        return json.data.map((item, index) => ({
            ...item,
            date: moment(item.date).format("MMM DD, YYYY"),
            revenue: item.revenue_ads
        }))
    }

    function filterDataBreakCountry(json) {
        return json.data.map((item, index) => ({
            country_name: item.country,
            data_by_date: item.data_by_date.map((item, index) => ({
                ...item,
                date: moment(item.date).format("MMM DD, YYYY"),
            })),
            revenue: item.revenue_ads
        }))
    }

    function filterDataBreakPlatform(json) {
        return json.data.map((item, index) => ({
            platform_name: item.platform,
            data_by_date: item.data_by_date.map((item, index) => ({
                ...item,
                date: moment(item.date).format("MMM DD, YYYY"),
            })),
            revenue: item.revenue_ads
        }))
    }

    function filterDataBreakNetwork(json) {
        return json.data.map((item, index) => ({
            network_name: item.network,
            data_by_date: item.data_by_date.map((item, index) => ({
                ...item,
                date: moment(item.date).format("MMM DD, YYYY"),
            })),
            revenue: item.revenue_ads
        }))
    }

    function filterDataBreakCompany(json) {
        return json.data.map((item, index) => ({
            company_name: item.company,
            data_by_date: item.data_by_date.map((item, index) => ({
                ...item,
                date: moment(item.date).format("MMM DD, YYYY"),
            })),
            revenue: item.revenue_ads
        }))
    }

    table.on('processing.dt', function (e, settings, processing) {
        if (processing) {
            $('.loading-panel').removeClass('inactive')
            showChartLoading();
        } else {
            showColumnByBreak();
            $('.loading-panel').addClass('inactive');
            hideChartLoading();
        }
    });

    table.on('xhr', function (e, settings, dataJson) {

        console.log('Ajax event occurred. Returned data: ', dataJson);

        let json = {};
        json.data = _.orderBy(dataJson.data, ['revenue_ads'], ['desc']);

        let chartStatus = Chart.getChart("myChart")

        if (chartStatus != undefined) {
            showChartLoading();
            chartStatus.destroy();
        }

        let data;
        let yAxisKey;
        let xAxisKey;
        let labelKey;
        let dataKey;

        const charTabActiveLable = {
            revenue_in_app: 'Revenue In App (Sum)',
            revenue_ads: 'Revenue Ads (Sum)',
            revenue_sum: 'Revenue All (Sum)',
            cost: 'Cost (Sum)',
            profit: 'Profit (Sum)',
        }

        switch ($('#p-breakby').val()) {
            case 'game_name':
                resetSlideByBreak([5, 6, 7])
                data = filterDataBreakGame(json);
                yAxisKey = chartTabActive;
                xAxisKey = 'date';
                labelKey = 'game_name';
                dataKey = 'data_by_date'
                break;
            case 'date':
                resetSlideByBreak([5, 6, 7])
                data = filterDataBreakDate(json);
                yAxisKey = chartTabActive;
                xAxisKey = 'date';
                labelKey = charTabActiveLable[chartTabActive];
                dataKey = '';
                break;
            case 'country_name':
                resetSlideByBreak([5, 6, 7])
                data = filterDataBreakCountry(json);
                yAxisKey = chartTabActive;
                xAxisKey = 'date';
                dataKey = 'data_by_date';
                labelKey = 'country_name';
                break;
            case 'platform':
                resetSlideByBreak([5, 6, 7])
                data = filterDataBreakPlatform(json);
                yAxisKey = chartTabActive;
                xAxisKey = 'date';
                dataKey = 'data_by_date';
                labelKey = 'platform_name';
                break;
            case 'network':
                resetSlideByBreak([0, 2, 3, 4, 5, 8]);
                data = filterDataBreakNetwork(json);
                yAxisKey = chartTabActive;
                xAxisKey = 'date';
                dataKey = 'data_by_date';
                labelKey = 'network_name';
                break;
            case 'company':
                resetSlideByBreak([5, 6, 7]);
                data = filterDataBreakCompany(json);
                yAxisKey = chartTabActive;
                xAxisKey = 'date';
                dataKey = 'data_by_date';
                labelKey = 'company_name';
                break;
            default:
                resetSlideByBreak([5, 6, 7]);
                data = filterDataBreakGame(json);
                yAxisKey = chartTabActive;
                xAxisKey = 'date';
                labelKey = 'game_name';
                dataKey = 'data_by_date'
                break;
        }

        console.log(data);

        function skipped(ctx, value, data) {
            return ctx.p1.parsed.y === 0 && ctx.p1DataIndex === data[0].data_by_date.length - 1 ? value : undefined;
        }

        const cfg = {
            type: 'line',
            data:
            {
                datasets: data.map((item, index) => {
                    let color = randomColor({
                        luminosity: 'random',
                        format: 'rgb'
                    });

                    return {
                        label: $('#p-breakby').val() !== 'date' ? labelKey ? item[labelKey] + (item.platform ? ' (' + item.platform + ')' : '') : '' : labelKey,
                        data: dataKey ? item[dataKey] : data,
                        parsing: {
                            yAxisKey,
                            xAxisKey
                        },
                        borderColor: color,
                        backgroundColor: color,
                        borderWidth: 2,
                        hidden: true,
                        revenue: item.revenue
                        // segment: {
                        //     borderDash: ctx => skipped(ctx, [6, 6], data)
                        // }
                    }
                }
                )
            },
            options: {
                responsive: true,
                maintainAspectRatio: false,
                scales: {
                    x: {
                        grid: {
                            display: false,
                        },
                        offset: true
                    },
                },
                plugins: {
                    legend: {
                        display: false
                    },
                    tooltip: {
                        callbacks: {
                            label: function (context) {
                                let label = context.dataset.label || '';

                                if (label) {
                                    label += ': ';
                                }
                                if (context.parsed.y !== null) {
                                    if (['impression', 'ecpm'].includes(context.dataset.parsing.yAxisKey)) {
                                        label += numeral(context.parsed.y).format('0,0');
                                    } else {
                                        label += numeral(context.parsed.y).format('$ 0,0.00');
                                    }
                                }
                                return label;
                            }
                        }
                    }
                },
            }
        };

        console.log(cfg);

        myChart = new Chart(
            document.getElementById('myChart'),
            cfg
        );

        // console.log(myChart);

        let currentData = [];
        switch ($('#p-breakby').val()) {
            case 'game_name':
                currentData = json.data.filter((item, index) => item.game !== 'sum');
                break;
            case 'date':
                currentData = json.data.filter((item, index) => item.date !== 'sum');
                break;
            case 'country_name':
                currentData = json.data.filter((item, index) => item.country !== 'sum');
                break;
            case 'platform':
                currentData = json.data.filter((item, index) => item.platform !== 'sum');
                break;
            case 'network':
                currentData = json.data.filter((item, index) => item.network !== 'sum');
                break;
            case 'company':
                currentData = json.data.filter((item, index) => item.company !== 'sum');
                break;
        }

        let totalRevenueInApp = _.sumBy(currentData, 'revenue_in_app')?.toFixed(2);
        let totalRevenueAds = _.sumBy(currentData, 'revenue_ads')?.toFixed(2);
        let totalRevenueAll = _.sumBy(currentData, 'revenue_sum')?.toFixed(2);
        let totalCost = _.sumBy(currentData, 'cost')?.toFixed(2);
        let totalProfit = _.sumBy(currentData, 'profit')?.toFixed(2);
        let totalEcpm = _.sumBy(currentData, 'ecpm')?.toFixed(2);
        let totalImpressions = _.sumBy(currentData, 'impression')?.toFixed(2);

        $('.stats-chart-item[data-tab-value="revenue_in_app"] .tab-sum-value').text(numeral(totalRevenueInApp).format('$ 0,0.00a'));
        $('.stats-chart-item[data-tab-value="revenue_ads"] .tab-sum-value').text(numeral(totalRevenueAds).format('$ 0,0.00a'));
        $('.stats-chart-item[data-tab-value="revenue_sum"] .tab-sum-value').text(numeral(totalRevenueAll).format('$ 0,0.00a'));
        $('.stats-chart-item[data-tab-value="cost"] .tab-sum-value').text(numeral(totalCost).format('$ 0,0.00a'));
        $('.stats-chart-item[data-tab-value="profit"] .tab-sum-value').text(numeral(totalProfit).format('$ 0,0.00a'));
        $('.stats-chart-item[data-tab-value="ecpm"] .tab-sum-value').text(numeral(totalEcpm).format('0,0.00a'));
        $('.stats-chart-item[data-tab-value="impression"] .tab-sum-value').text(numeral(totalImpressions).format('0,0.00a'));

        $('#custom-style-checkbox').text('');

        let styleCheckbox = '';

        myChart.data.datasets.forEach((item, index) => {
            styleCheckbox += `.label-chart-${index}:checked {background-color: ${item.backgroundColor}; border-color: ${item.borderColor}}`
            $('#custom-style-checkbox').text(styleCheckbox);
        });

        showChartLabelList(myChart);

        $('.stats-chart-item').each((index, item) => {
            $(item).click(() => {
                showChartLoading();
                chartTabActive = $(item).data('tabValue');

                if (myChart) {
                    myChart.data.datasets.forEach(dataset => {
                        dataset.parsing.yAxisKey = chartTabActive;
                        if ($('#p-breakby').val() === 'date') {
                            dataset.label = charTabActiveLable[chartTabActive];
                        }
                    });
                    myChart.update();
                    setTimeout(() => {
                        myChart.options.scales.y.ticks.stepSize = Math.floor(myChart.scales.y.max / 4);
                        myChart.update();
                    }, 0);
                }

                $(item).addClass('active');
                $(item).parent().siblings().find('.stats-chart-item').removeClass('active');
                hideChartLoading();
            })
        })

        $('.stats-chart-item')[0].click();

        if ($('#p-breakby').val() !== 'date') {
            $('.chart-list-item').each(function (index, item) {
                if (index <= 9) {
                    $(item).prop('checked', true).change();
                }
            })
        }

        if ($('#p-breakby').val() === 'date') {
            myChart.show(0);
            myChart.update();
        }

        $('.chart-sidebar').css('max-height', ($('.chart-wrap').outerHeight() + $('.slider-stats-chart').outerHeight()) + 'px');
        hideChartLoading();
    });

    $('#example tbody').on('click', 'td.dt-control', function () {
        var tr = $(this).closest('tr');
        var row = table.row(tr);

        if (row.child.isShown()) {
            // This row is already open - close it
            row.child.hide();
            tr.removeClass('shown');
        } else {
            // Open this row
            row.child(format(row.data())).show();
            tr.addClass('shown');
        }
    });

    $('.format-currency').each((index, item) => {
        $(item).text('$' + numeral($(item).text()).format('0,0.00'));
    });

    $('.btn-submit').click(() => {
        table.ajax.reload();
    });

    $('.btn-date-before').on('click', '', function () {
        $('#Sdate').val(moment($('#Sdate').val()).subtract(1, 'd').format('YYYY-MM-DD'));
        $('#Edate').val(moment($('#Edate').val()).subtract(1, 'd').format('YYYY-MM-DD'));
        var drp = $('#date-range').data('daterangepicker');
        drp.setStartDate($('#Sdate').val());
        drp.setEndDate($('#Edate').val());
        table.ajax.reload();
    });
    $('.btn-date-after').on('click', '', function () {
        $('#Sdate').val(moment($('#Sdate').val()).add(1, 'd').format('YYYY-MM-DD'));
        $('#Edate').val(moment($('#Edate').val()).add(1, 'd').format('YYYY-MM-DD'));
        var drp = $('#date-range').data('daterangepicker');
        drp.setStartDate($('#Sdate').val());
        drp.setEndDate($('#Edate').val());
        table.ajax.reload();
    });

    $('.btn-filter-submit').each((index, element) => {
        $(element).click(() => {
            table.ajax.reload();
        })
    });

    $('#example tbody').on('dblclick', 'tr', function () {
        let dataRow = table.row(this).data();
        let breakValue = $('#p-breakby').val();

        switch (breakValue) {
            case 'game_name':
                if ($('#p-game-name').val().split(',').includes(dataRow.game_id.toString())) {
                    $('#filter-app .btn-filter-clear').click();
                } else {
                    $('#filter-app .btn-filter-clear').click();
                    $('#filter-app .name-app#' + dataRow.game_id).click();
                }
                setTimeout(() => {
                    $('.btn-submit').click();
                }, 0);

                break;
            case 'country_name':
                if ($('#p-country').val().split(',').includes(`'${dataRow.country_code}'`)) {
                    $('#filter-country .btn-filter-clear').click();
                } else {
                    $('#filter-country .btn-filter-clear').click();
                    $($('#filter-country .name-app#' + dataRow.country_code)[0]).click();
                }
                setTimeout(() => {
                    $('.btn-submit').click();
                }, 0);
                break;
            case 'platform':
                if ($('#p-platform').val().split(',').includes(`'${dataRow.platform}'`)) {
                    $('#p-platform').val('');
                } else {
                    $('#p-platform').val(`'${dataRow.platform}'`);
                }
                setTimeout(() => {
                    $('.btn-submit').click();
                }, 0);
                break;
            case 'network':
                if ($('#p-adsource').val().split(',').includes(dataRow.network)) {
                    $('#filter-network .btn-filter-clear').click();
                } else {
                    $('#filter-network .btn-filter-clear').click();
                    $($('#filter-network .name-app#' + dataRow.network)[0]).click();
                }
                setTimeout(() => {
                    $('.btn-submit').click();
                }, 0);
                break;
            case 'company':
                if ($('#p-company').val().split(',').includes(dataRow.company)) {
                    $('#filter-company .btn-filter-clear').click();
                } else {
                    $('#filter-company .btn-filter-clear').click();
                    $($('#filter-company .name-app#' + dataRow.company)[0]).click();
                }
                setTimeout(() => {
                    $('.btn-submit').click();
                }, 0);
                break;
            default:
                if ($('#p-game-name').val().split(',').includes(dataRow.game_id.toString())) {
                    $('#filter-app .btn-filter-clear').click();
                } else {
                    $('#filter-app .btn-filter-clear').click();
                    $('#filter-app .name-app#' + dataRow.game_id).click();
                }
                setTimeout(() => {
                    $('.btn-submit').click();
                }, 0);
                break;
        }
    });

    $('.btn-sort-label').each((index, item) => {
        $(item).click(() => {
            let sortBy = $(item).data('sortby')
            $(item).siblings().removeClass('active');
            $(item).addClass('active');

            let listLabelChart = $('.chart-sidebar-list .form-check');
            let dataSort = [];
            listLabelChart.each((index, item) => {
                dataSort.push({
                    label: $(item).find('.form-check-label').text().trim(),
                    revenue: $(item).data('revenue'),
                    elementId: $(item).attr('id'),
                })
            })



            if (sortBy === 'revenue') {
                let result = _.orderBy(dataSort, ['revenue'], ['desc']);
                result.forEach((item, index) => {
                    $('#' + item.elementId).css('order', index + 1);
                })

                return;
            }

            if (sortBy === 'abc') {
                let result = _.orderBy(dataSort, ['label'], ['asc']);
                result.forEach((item, index) => {
                    $('#' + item.elementId).css('order', index + 1);
                })
                return;
            }


        });
    });

    function showChartLoading() {
        $('.chart-loading').addClass('active');
    }

    function hideChartLoading() {
        $('.chart-loading').removeClass('active');
    }
});